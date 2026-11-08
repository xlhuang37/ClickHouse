#pragma once

#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceSchedulingContext.h>
#include <Common/Scheduler/CostUnit.h>
#include <Common/Scheduler/Debug.h>
#include <Common/Scheduler/Nodes/TimeShared/GittinsIndexData.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/BitHelpers.h>

#include <boost/intrusive/set.hpp>

#include <algorithm>
#include <atomic>
#include <array>
#include <cmath>
#include <exception>
#include <iterator>
#include <limits>
#include <mutex>
#include <string_view>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
    extern const int SERVER_OVERLOADED;
    extern const int BAD_ARGUMENTS;
}

/// Which scheduling algorithm a `RequestQueue` leaf runs. Selected by the WORKLOAD setting
/// `scheduler`. `fifo` reproduces the historical first-come-first-served behaviour and is the default.
enum class SchedulerAlgorithm
{
    Fifo, /// First-come-first-served by request enqueue order
    Fcfs, /// Queries ordered by the sequence assigned when the query first submits a request
    IfFcfs, /// `fcfs`, except a CPU slot ceiling of 1 or 2 is served strictly first
    Fair, /// Weighted fair queueing (SFQ) with per-query weight lowering
    Las, /// Least-Attained-Service, MLFQ-bucketed (favours short queries; can starve long ones)
    Gittins, /// Gittins index for the base distribution, bucketed into normalized powers-of-two levels
    MemGittins, /// Memory-conditional Gittins index, bucketed into normalized powers-of-two levels
    GittinsRaw, /// Gittins index for the base distribution, ordered by the raw index
    MemGittinsRaw, /// Memory-conditional Gittins index, ordered by the raw index
    Priority, /// Strict priority by the query's `priority` setting (lower value first; can starve)
    Srpt, /// Shortest Remaining Response Time from expected runtime minus attained CPU service
    FairMax, /// New queries start at the highest attained service; less-served queries stay ahead
    FairTier5, /// FCFS by query sequence inside fixed 5-second attained-service tiers
    FairTier10, /// FCFS by query sequence inside fixed 10-second attained-service tiers
};

inline SchedulerAlgorithm parseSchedulerAlgorithm(const String & name)
{
    if (name.empty() || name == "fifo")
        return SchedulerAlgorithm::Fifo;
    if (name == "fcfs")
        return SchedulerAlgorithm::Fcfs;
    if (name == "if-fcfs")
        return SchedulerAlgorithm::IfFcfs;
    if (name == "fair")
        return SchedulerAlgorithm::Fair;
    if (name == "las")
        return SchedulerAlgorithm::Las;
    if (name == "gittins")
        return SchedulerAlgorithm::Gittins;
    if (name == "mem-gittins")
        return SchedulerAlgorithm::MemGittins;
    if (name == "gittins-raw")
        return SchedulerAlgorithm::GittinsRaw;
    if (name == "mem-gittins-raw")
        return SchedulerAlgorithm::MemGittinsRaw;
    if (name == "priority")
        return SchedulerAlgorithm::Priority;
    if (name == "srpt")
        return SchedulerAlgorithm::Srpt;
    if (name == "fair-max")
        return SchedulerAlgorithm::FairMax;
    if (name == "fair-tier-5")
        return SchedulerAlgorithm::FairTier5;
    if (name == "fair-tier-10")
        return SchedulerAlgorithm::FairTier10;
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown workload scheduler '{}' (expected 'fifo', 'fcfs', 'if-fcfs', 'fair', 'las', 'gittins', 'mem-gittins', "
        "'gittins-raw', 'mem-gittins-raw', 'priority', 'srpt', 'fair-max', 'fair-tier-5' or 'fair-tier-10')",
        name);
}

/// Pluggable ordering strategy owned by a `RequestQueue`. It owns only the container of pending
/// requests and the ordering; the enclosing `RequestQueue` owns all the cross-cutting concerns
/// (mutex, budget, `max_waiting_queries`, counters, activation). All methods are called under the
/// `RequestQueue` mutex.
class ISchedulingAlgorithm
{
public:
    virtual ~ISchedulingAlgorithm() = default;

    /// Insert a pending request (computing its ordering key from the query context if needed).
    virtual void push(ResourceRequest * request) = 0;

    /// Remove and return the next request to serve, or nullptr if empty.
    virtual ResourceRequest * pop() = 0;

    /// Remove a specific request if it is enqueued in this algorithm. Returns true iff removed.
    virtual bool erase(ResourceRequest * request) = 0;

    /// Remove and return the least-preferred request (served last), or nullptr if empty.
    /// Used to trim to `max_waiting_queries`.
    virtual ResourceRequest * popWorst() = 0;

    /// Move all pending requests, in order, to the back of `out`. Never allocates: `out` links them
    /// through `enqueued_hook`, which is free once a request leaves an algorithm.
    virtual void pullAll(ResourceRequest::EnqueuedList & out) noexcept = 0;

    /// Restart the ordering state (virtual time, arrival sequence) of the empty algorithm, as if new.
    virtual void reset() noexcept = 0;

    virtual bool empty() const = 0;
};

/// `fifo` — first-come-first-served. Byte-for-byte the historical leaf ordering.
class FifoAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override { requests.push_back(*request); }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        ResourceRequest * request = &requests.front();
        requests.pop_front();
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->enqueued_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        ResourceRequest * request = &requests.back();
        requests.pop_back();
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override { out.splice(out.end(), requests); }

    void reset() noexcept override {}

    bool empty() const override { return requests.empty(); }

private:
    ResourceRequest::EnqueuedList requests;
};

/// `fcfs` — queries ordered by `ResourceQueryState::fcfs_sequence`, assigned when the query's first
/// request is accepted on this leaf. Every request of that query carries the same sequence, so a
/// later quantum of an earlier query stays ahead of a query that submitted afterwards. A popped
/// quantum stays with that query until `finish()`. While a lower sequence holds any granted quantum,
/// a higher sequence stays queued.
class FcfsAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override
    {
        chassert(request->scheduling.state->fcfs_sequence_assigned);
        // The query sequence is the only ordering key. Requests of one query compare equal and the
        // multiset keeps them in enqueue order.
        request->scheduling.key = {0.0, request->scheduling.state->fcfs_sequence};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        // A lower sequence still holds a granted quantum. Leave higher sequences queued so the
        // scheduler does not hand the next slot to them.
        const UInt64 blocked_until = earliestInflightSequence();
        if (blocked_until < requests.begin()->scheduling.key.second)
            return nullptr;
        auto it = requests.begin();
        ResourceRequest * request = &*it;
        requests.erase(it);
        noteGranted(*request);
        return request;
    }

    /// `finish()` of a granted quantum. Returns true when this was the query's last in-flight quantum.
    bool releaseInflight(ResourceQueryState * state)
    {
        chassert(state->fcfs_inflight > 0);
        if (--state->fcfs_inflight > 0)
            return false;
        std::erase(inflight_states, state);
        return true;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
    }

    bool empty() const override { return requests.empty(); }

private:
    void noteGranted(ResourceRequest & request)
    {
        auto * state = request.scheduling.state;
        if (state->fcfs_inflight == 0)
            inflight_states.push_back(state);
        ++state->fcfs_inflight;
    }

    /// Smallest query sequence that still holds a granted quantum, or max if none do.
    UInt64 earliestInflightSequence() const noexcept
    {
        UInt64 earliest = std::numeric_limits<UInt64>::max();
        for (const ResourceQueryState * state : inflight_states)
            if (state->fcfs_inflight > 0)
                earliest = std::min(earliest, state->fcfs_sequence);
        return earliest;
    }

    struct ByQuerySequence
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key.second < rhs.scheduling.key.second;
        }
    };
    using Set = boost::intrusive::multiset<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByQuerySequence>>;

    Set requests;
    /// Queries with a non-zero `fcfs_inflight` on this leaf. Entries are removed when the count hits zero.
    std::vector<ResourceQueryState *> inflight_states;
};

/// `if-fcfs` — `fcfs` with a strict high-priority class for small CPU ceilings. A query whose
/// published `max_cpu_slots` is 1 or 2 is served ahead of every other query. A ceiling of 0
/// (unpublished) or above 2 stays in the normal class. Inside each class the order is
/// `fcfs_sequence`, and a later sequence in that class stays queued while an earlier sequence of
/// the same class holds a granted quantum. A privileged request is not held back by a
/// non-privileged quantum. The normal class is not admitted while a privileged request is queued
/// or a privileged quantum is still in flight. The pipeline ceiling only grows, so a queued
/// request can leave the privileged class; `pop` re-keys the front request when that happens.
class IfFcfsAlgorithm final : public ISchedulingAlgorithm
{
public:
    static constexpr UInt64 max_privileged_slots = 2;

    void push(ResourceRequest * request) override
    {
        chassert(request->scheduling.state->fcfs_sequence_assigned);
        auto & state = *request->scheduling.state;
        // Class, then the query sequence. Requests of one query in one class compare equal.
        request->scheduling.key = {classOf(state), state.fcfs_sequence};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        while (!requests.empty())
        {
            auto it = requests.begin();
            ResourceRequest * request = &*it;
            const double live_class = classOf(*request->scheduling.state);
            // The ceiling grew past 2 while this request sat in the privileged class. Move it
            // behind queries that are still privileged and look at the new front.
            if (live_class > request->scheduling.key.first)
            {
                requests.erase(it);
                request->scheduling.key.first = live_class;
                requests.insert(*request);
                continue;
            }
            if (isBlocked(*request))
                return nullptr;
            requests.erase(it);
            noteGranted(*request);
            return request;
        }
        return nullptr;
    }

    /// `finish()` of a granted quantum. Returns true when this was the query's last in-flight quantum.
    bool releaseInflight(ResourceQueryState * state)
    {
        chassert(state->fcfs_inflight > 0);
        if (--state->fcfs_inflight > 0)
            return false;
        std::erase(inflight_states, state);
        return true;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
    }

    bool empty() const override { return requests.empty(); }

private:
    static bool isPrivileged(const ResourceQueryState & state) noexcept
    {
        const UInt64 slots = state.max_cpu_slots.load(std::memory_order_acquire);
        return slots >= 1 && slots <= max_privileged_slots;
    }

    static double classOf(const ResourceQueryState & state) noexcept
    {
        return isPrivileged(state) ? 0.0 : 1.0;
    }

    /// A privileged front request waits only for an earlier privileged quantum. A normal front
    /// request also waits for every privileged quantum still in flight, and for an earlier normal one.
    bool isBlocked(const ResourceRequest & request) const
    {
        const ResourceQueryState & state = *request.scheduling.state;
        const bool privileged = isPrivileged(state);
        const UInt64 sequence = state.fcfs_sequence;
        for (const ResourceQueryState * inflight : inflight_states)
        {
            if (inflight->fcfs_inflight == 0)
                continue;
            const bool inflight_privileged = isPrivileged(*inflight);
            if (privileged)
            {
                if (inflight_privileged && inflight->fcfs_sequence < sequence)
                    return true;
            }
            else if (inflight_privileged || inflight->fcfs_sequence < sequence)
            {
                return true;
            }
        }
        return false;
    }

    void noteGranted(ResourceRequest & request)
    {
        auto * state = request.scheduling.state;
        if (state->fcfs_inflight == 0)
            inflight_states.push_back(state);
        ++state->fcfs_inflight;
    }

    /// Class, then the query's `fcfs_sequence`. Equal keys stay in enqueue order via the multiset.
    struct ByClassAndQuerySequence
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::multiset<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByClassAndQuerySequence>>;

    Set requests;
    /// Queries with a non-zero `fcfs_inflight` on this leaf. Entries are removed when the count hits zero.
    std::vector<ResourceQueryState *> inflight_states;
};

/// `fair` — weighted fair queueing (Start-time Fair Queueing). Requests are ordered by a virtual
/// runtime key; a newly active query starts at the system virtual time so it is not penalised for
/// idle periods (no starvation). A query's effective weight is lowered once it crosses an age or
/// attained-service threshold (see the query settings), which biases the fair shares toward
/// shorter/newer queries.
class FairAlgorithm final : public ISchedulingAlgorithm
{
public:
    explicit FairAlgorithm(CostUnit unit_)
        : unit(unit_)
    {
    }

    void push(ResourceRequest * request) override
    {
        chassert(request->scheduling.context); // set by the classifier that issued the link
        // A request arriving to an empty queue starts a new busy period: roll system virtual time to
        // the max finish tag reached so far, so a query idle across the boundary (or a fresh query)
        // does not start behind a stale system time and monopolise the resource until it catches up.
        if (requests.empty())
            system_vruntime = std::max(system_vruntime, max_vruntime);
        auto & state = *request->scheduling.state;
        double effective_weight = updateEffectiveWeight(*request->scheduling.context, state);
        // Charge the declared cost plus any pending real-vs-estimate correction — folded here from
        // fair's own accumulator, independent of attained_cost; never negative, so vruntime only
        // moves forward.
        ResourceCost charge = state.fair.drainVruntimeCorrection(request->scheduling.cost);
        double vstart = std::max(system_vruntime, state.fair.vruntime);
        double increment = static_cast<double>(charge) / effective_weight;
        state.fair.vruntime = vstart + increment;
        request->scheduling.vruntime_increment = increment; // kept so cancel() can undo this projection
        max_vruntime = std::max(max_vruntime, state.fair.vruntime);
        request->scheduling.key = {vstart, next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        auto it = requests.begin();
        ResourceRequest * request = &*it;
        requests.erase(it);
        // System virtual time advances to the start tag of the served request (monotonic).
        // attained_cost is charged generically in RequestQueue::dequeueRequest.
        system_vruntime = std::max(system_vruntime, request->scheduling.key.first);
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        rollbackProjection(request); // cancellation: the request will not run
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        rollbackProjection(request); // eviction (`max_waiting_queries` shrink): the request will not run
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
        system_vruntime = 0.0;
        max_vruntime = 0.0;
        next_seq = 0;
    }

    bool empty() const override { return requests.empty(); }

private:
    /// Undo the virtual runtime a request projected onto its query at push(), for a request that will
    /// not run (cancelled, or evicted by a `max_waiting_queries` shrink), so the query is not billed
    /// for service it never received (which would delay its future requests). Exact for the query's
    /// most recent projection (the common case — one pending request removed). A non-tail removal does
    /// not re-key the query's already-queued later requests, so they keep their higher keys while a
    /// subsequent request of the same query is keyed from the lowered runtime and may run ahead of them
    /// — a bounded, within-query reordering. (The drained finish-correction folded into the projection
    /// is not restored either; kept simple.)
    static void rollbackProjection(ResourceRequest * request)
    {
        request->scheduling.state->fair.vruntime -= request->scheduling.vruntime_increment;
    }

    /// Fair effective weight: the query's `weight`, lowered once by `weight_lowering_factor` the
    /// first time it crosses any configured threshold (thresholds do not combine — the first to trip
    /// applies the full lowering). Lowering is a one-way latch (`weight_lowered`): the cached
    /// `effective_weight` is reused afterwards, so the threshold checks run only until the crossing.
    /// Called at push(), so lowering biases the query's subsequent requests — a request already keyed
    /// keeps its weight; the bounded lag is fine for a single-step bias (unlike `las`, which re-keys
    /// on pop()).
    double updateEffectiveWeight(const ResourceSchedulingContext & ctx, ResourceQueryState & state) const
    {
        if (!state.fair.weight_lowered)
        {
            // Fast path: when lowering can never change the weight — the factor is 1 (disabled) or no
            // threshold is set — finalize `effective_weight` on the first push and skip the per-push
            // threshold check (and its clock read) for the rest of the query.
            const bool lowering_disabled = ctx.weight_lowering_factor >= 1.0
                || (ctx.weight_lowering_age_seconds <= 0 && ctx.weight_lowering_cpu_seconds <= 0
                    && ctx.weight_lowering_io_bytes <= 0);
            if (lowering_disabled)
            {
                state.fair.weight_lowered = true;
                state.fair.effective_weight = ctx.weight > 0 ? ctx.weight : 1e-9;
            }
            else if (weightLoweringThresholdCrossed(ctx, state))
            {
                state.fair.weight_lowered = true;
                double weight = ctx.weight * ctx.weight_lowering_factor;
                state.fair.effective_weight = weight > 0 ? weight : 1e-9; // guard against division by zero
            }
            else
            {
                state.fair.effective_weight = ctx.weight > 0 ? ctx.weight : 1e-9;
            }
        }
        return state.fair.effective_weight;
    }

    /// True once the query's real cumulative service crosses a `weight_lowering_*` threshold.
    /// `attained_cost` already tracks real service (finish folds its correction straight in). For CPU
    /// it is granted service and leads spent CPU by at most one quantum.
    bool weightLoweringThresholdCrossed(const ResourceSchedulingContext & ctx, const ResourceQueryState & state) const
    {
        if (ctx.weight_lowering_age_seconds > 0)
        {
            UInt64 now = clock_gettime_ns();
            double age_seconds = now > ctx.start_ns ? static_cast<double>(now - ctx.start_ns) / 1e9 : 0.0;
            if (age_seconds >= ctx.weight_lowering_age_seconds)
                return true;
        }
        const Int64 attained_service = state.attained_cost.load(std::memory_order_relaxed);
        if (unit == CostUnit::CPUNanosecond && ctx.weight_lowering_cpu_seconds > 0
            && static_cast<double>(attained_service) / 1e9 >= ctx.weight_lowering_cpu_seconds)
            return true;
        if (unit == CostUnit::IOByte && ctx.weight_lowering_io_bytes > 0
            && static_cast<double>(attained_service) >= ctx.weight_lowering_io_bytes)
            return true;
        return false;
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    const CostUnit unit;
    double system_vruntime = 0.0;
    // Max finish tag assigned so far (max `vstart + charge/weight` over pushed requests). At a
    // busy-period boundary system virtual time rolls forward to this (SFQ rollover; mirrors
    // FairPolicy.h's `max_vruntime`), so an idle/fresh query cannot start at a stale, lower time.
    double max_vruntime = 0.0;
    UInt64 next_seq = 0;
    Set requests;
};

/// `fair-max` — a query's virtual time starts at the highest attained service in the queue, then
/// grows by each request's declared cost. Lower virtual time is served first, and an equal value
/// joins the tail. A query that has been served less stays ahead of one that has caught the
/// maximum. Weight and weight lowering are not applied. `pop` does not move the system value.
class FairMaxAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override
    {
        chassert(request->scheduling.context);
        auto & state = request->scheduling.state->fair_max;
        if (!state.initialized)
        {
            state.vruntime = system_vruntime;
            state.initialized = true;
        }
        const double vstart = state.vruntime;
        const double increment = static_cast<double>(request->scheduling.cost);
        state.vruntime += increment;
        if (state.vruntime > system_vruntime)
            system_vruntime = state.vruntime;
        request->scheduling.vruntime_increment = increment;
        request->scheduling.key = {vstart, next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        auto it = requests.begin();
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        rollbackProjection(request);
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        rollbackProjection(request);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
        system_vruntime = 0.0;
        next_seq = 0;
    }

    bool empty() const override { return requests.empty(); }

private:
    static void rollbackProjection(ResourceRequest * request)
    {
        request->scheduling.state->fair_max.vruntime -= request->scheduling.vruntime_increment;
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    double system_vruntime = 0.0;
    UInt64 next_seq = 0;
    Set requests;
};

/// `las` — practical Least-Attained-Service, MLFQ-bucketed. Serves the query that has attained the
/// least service in this resource first (best mean latency under heavy-tailed query sizes),
/// coarsened into geometric levels (`level = floor(log2(1 + attained/base))`) so a query drops one
/// level each time its attained service doubles — bounding reordering churn. Lowest level served
/// first, FIFO within a level. Pure: there is no starvation guard, so a long-running query can be
/// starved by a continuous stream of short ones — use `fair` when a no-starvation guarantee is
/// needed.
class LasAlgorithm final : public ISchedulingAlgorithm
{
public:
    explicit LasAlgorithm(CostUnit unit_)
        : base(baseQuantum(unit_))
    {
    }

    void push(ResourceRequest * request) override
    {
        Int64 attained = request->scheduling.state->attained_cost.load(std::memory_order_relaxed);
        request->scheduling.key = {levelOf(attained), next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        // Lazy re-keying: a request's stored level (levelOf(attained) at enqueue) only goes stale
        // too LOW as the query's other requests accrue service. On pop, if the front request's true
        // level (from current attained) exceeds its key, re-key and defer it; otherwise serve it.
        // Restores bucketed LAS when a query has several requests queued at once (IO), a no-op with
        // one (CPU). Bounded to ~log2(total_service) re-keys per request; the FIFO tie-break is kept.
        while (!requests.empty())
        {
            auto it = requests.begin();
            ResourceRequest * request = &*it;
            // `attained_cost` already tracks real service (finish folds its correction straight in),
            // so re-key from its current value: if the query's accrued service has pushed the true
            // level past this request's stored key, re-key and defer it; otherwise serve it.
            UInt32 real_level = levelOf(request->scheduling.state->attained_cost.load(std::memory_order_relaxed));
            if (real_level > request->scheduling.key.first)
            {
                requests.erase(it);
                request->scheduling.key.first = real_level;
                requests.insert(*request);
                continue;
            }
            requests.erase(it);
            // attained_cost is charged generically in RequestQueue::dequeueRequest.
            return request;
        }
        return nullptr;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
        next_seq = 0;
    }

    bool empty() const override { return requests.empty(); }

private:
    /// Internal per-resource quantum used as the bucketing unit (not exposed as a SQL setting).
    static ResourceCost baseQuantum(CostUnit unit_)
    {
        switch (unit_)
        {
            case CostUnit::CPUNanosecond: return 10'000'000; // ~10 ms CPU lease quantum
            case CostUnit::IOByte: return 1'048'576; // 1 MiB
            case CostUnit::QuerySlot: return 1;
            case CostUnit::MemoryByte: return 1;
        }
        return 1;
    }

    UInt32 levelOf(Int64 attained) const
    {
        if (attained <= 0)
            return 0;
        // MLFQ level = floor(log2(1 + attained/base)). Since 1 + attained/base = (attained + base)/base
        // and floor(log2(floor(r))) == floor(log2(r)) for r >= 1, the level is the index of the highest
        // set bit of the integer quotient — a single bit-scan, avoiding FP log2/floor on the hot path.
        UInt64 quotient = (static_cast<UInt64>(attained) + static_cast<UInt64>(base)) / static_cast<UInt64>(base);
        return bitScanReverse(quotient);
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    const ResourceCost base;
    UInt64 next_seq = 0;
    Set requests;
};

/// `fair-tier-5` and `fair-tier-10` — first-come, first-served inside fixed attained-service tiers.
/// The step is `5'000'000'000` or `10'000'000'000` cost units (5 or 10 CPU-seconds, or 5 or 10 GiB
/// on an IO resource). A lower tier is served first. A query first seen while other queries are
/// active joins their lowest tier, then moves up one tier for every further step of its own attained
/// service. Within a tier the only key is `fcfs_sequence`: the query that first submitted a request
/// stays ahead of one that submitted later, including after a requeue. Once a query crosses into the
/// next tier, a queued request is re-keyed and yields to queries still in an earlier tier.
class FairTierAlgorithm final : public ISchedulingAlgorithm
{
public:
    static constexpr ResourceCost tier_5_cost = 5'000'000'000;
    static constexpr ResourceCost tier_10_cost = 10'000'000'000;

    explicit FairTierAlgorithm(ResourceCost tier_cost_)
        : tier_cost(tier_cost_)
    {
    }

    void push(ResourceRequest * request) override
    {
        chassert(request->scheduling.context);
        auto & state = *request->scheduling.state;
        forgetInService(&state);
        if (!state.fair_tier.initialized)
        {
            state.fair_tier.base_tier = lowestActiveTier();
            state.fair_tier.initialized = true;
        }
        chassert(state.fcfs_sequence_assigned);
        // Tier, then the query sequence. Requests of one query in one tier compare equal.
        request->scheduling.key = {effectiveTier(state), state.fcfs_sequence};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        while (!requests.empty())
        {
            auto it = requests.begin();
            ResourceRequest * request = &*it;
            const double real_tier = effectiveTier(*request->scheduling.state);
            if (real_tier > request->scheduling.key.first)
            {
                requests.erase(it);
                request->scheduling.key.first = real_tier;
                requests.insert(*request);
                continue;
            }
            requests.erase(it);
            noteInService(request->scheduling.state);
            return request;
        }
        return nullptr;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
        in_service.clear();
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
        in_service.clear();
    }

    bool empty() const override { return requests.empty(); }

private:
    double tierOf(Int64 attained) const
    {
        if (attained <= 0)
            return 0;
        return static_cast<double>(static_cast<UInt64>(attained) / static_cast<UInt64>(tier_cost));
    }

    double effectiveTier(const ResourceQueryState & state) const
    {
        const Int64 attained = state.attained_cost.load(std::memory_order_relaxed);
        return state.fair_tier.base_tier + tierOf(attained);
    }

    /// Lowest tier of a query that is queued or was popped and has not requeued. An idle queue with
    /// nothing in service is tier 0. Read from current `attained_cost`, not the stored key: a queued
    /// request's key stays at the tier it had when it was pushed.
    double lowestActiveTier() const
    {
        double lowest = std::numeric_limits<double>::infinity();
        for (const ResourceRequest & request : requests)
            lowest = std::min(lowest, effectiveTier(*request.scheduling.state));
        for (const ResourceQueryState * state : in_service)
            lowest = std::min(lowest, effectiveTier(*state));
        if (lowest == std::numeric_limits<double>::infinity())
            return 0;
        return lowest;
    }

    void noteInService(ResourceQueryState * state)
    {
        if (std::find(in_service.begin(), in_service.end(), state) == in_service.end())
            in_service.push_back(state);
    }

    void forgetInService(ResourceQueryState * state)
    {
        std::erase(in_service, state);
    }

    /// Tier, then the query's `fcfs_sequence`. Equal keys stay in enqueue order via the multiset.
    struct ByTierAndQuerySequence
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::multiset<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByTierAndQuerySequence>>;

    const ResourceCost tier_cost;
    Set requests;
    /// Queries whose latest request was popped and that have not pushed again. Their attained tier
    /// stays visible while the queue is empty, so a new arrival joins them instead of tier 0.
    std::vector<ResourceQueryState *> in_service;
};

/// `srpt` — experimental Shortest Remaining Response Time for CPU resources. A query's priority
/// is its expected runtime minus attained CPU service; lower remaining time is served first, FIFO
/// on ties. An expected runtime of zero means unknown and deliberately gives the query high
/// priority. Unlike LAS, attained service makes this key decrease, so stale requests may be buried
/// anywhere in the tree. Refresh every queued key before selecting an endpoint to keep the
/// red-black-tree ordering valid.
class SrptAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override
    {
        request->scheduling.key = {remainingService(*request), next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        refreshKeys();
        if (requests.empty())
            return nullptr;
        auto it = requests.begin();
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        refreshKeys();
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        refreshKeys();
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
        next_seq = 0;
    }

    bool empty() const override { return requests.empty(); }

private:
    static double remainingService(const ResourceRequest & request)
    {
        chassert(request.scheduling.context);
        constexpr double nanoseconds_per_microsecond = 1e3;
        const double expected = request.scheduling.context->expected_query_runtime * nanoseconds_per_microsecond;
        const Int64 attained = request.scheduling.state->attained_cost.load(std::memory_order_relaxed);
        return expected - static_cast<double>(attained);
    }

    void refreshKeys() noexcept
    {
        for (auto it = requests.begin(); it != requests.end();)
        {
            ResourceRequest * request = &*it;
            ++it;
            const double current_key = remainingService(*request);
            if (current_key != request->scheduling.key.first)
            {
                requests.erase(requests.iterator_to(*request));
                request->scheduling.key.first = current_key;
                requests.insert(*request);
            }
        }
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    UInt64 next_seq = 0;
    Set requests;
};

/// Gittins policy. Attained CPU service chooses a 100 ms row. The naive policy always uses the base
/// distribution (memory above 0 GiB); the memory-conditioned policy chooses a distribution column
/// from peak per-query memory. The standard variants normalize raw indices once at construction:
/// zero is level 0, the global smallest positive index is level 1, and every doubling adds a level.
/// The raw variants order directly by the table value. Higher values are served first, FIFO on ties.
/// As in LAS, a queued request is lazily re-keyed before service so completed CPU quanta and, when
/// applicable, peak memory are reflected without mutating set keys.
class GittinsAlgorithm final : public ISchedulingAlgorithm
{
public:
    explicit GittinsAlgorithm(CostUnit unit_, bool memory_conditioned_, bool normalize_indices_)
        : unit(unit_)
        , memory_conditioned(memory_conditioned_)
        , normalize_indices(normalize_indices_)
    {
    }

    void push(ResourceRequest * request) override
    {
        request->scheduling.key = {-priorityOf(*request), next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        while (!requests.empty())
        {
            auto it = requests.begin();
            ResourceRequest * request = &*it;
            const double current_key = -priorityOf(*request);
            if (current_key != request->scheduling.key.first)
            {
                requests.erase(it);
                request->scheduling.key.first = current_key;
                requests.insert(*request);
                continue;
            }
            requests.erase(it);
            return request;
        }
        return nullptr;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
        next_seq = 0;
    }

    bool empty() const override { return requests.empty(); }

private:
    static constexpr size_t base_memory_band = 0;

    double priorityOf(ResourceRequest & request) const
    {
        auto & state = *request.scheduling.state;
        size_t memory_band = base_memory_band;
        if (memory_conditioned)
        {
            Int64 memory_usage = state.memory_usage.load(std::memory_order_relaxed);
            if (request.scheduling.context && request.scheduling.context->memory_tracker)
            {
                memory_usage = std::max<Int64>(0, request.scheduling.context->memory_tracker->getPeak());
                state.memory_usage.store(memory_usage, std::memory_order_relaxed);
            }
            memory_band = memoryBand(memory_usage);
        }

        const Int64 attained = state.attained_cost.load(std::memory_order_relaxed);
        size_t attained_bucket = 0;
        if (unit == CostUnit::CPUNanosecond && attained > 0)
            attained_bucket = std::min<UInt64>(
                static_cast<UInt64>(attained) / 100'000'000ULL, gittins_attained_buckets - 1);
        if (normalize_indices)
            return static_cast<double>(normalizedLevels()[memory_band][attained_bucket]);
        return gittins_indices[attained_bucket][memory_band];
    }

    using Levels = std::array<std::array<UInt8, gittins_attained_buckets>, gittins_memory_bands>;

    static const Levels & normalizedLevels()
    {
        static const Levels levels = []
        {
            Levels result{};
            double min_positive = std::numeric_limits<double>::max();
            for (const auto & row : gittins_indices)
                for (double index : row)
                    if (index > 0)
                        min_positive = std::min(min_positive, index);

            for (size_t attained = 0; attained < gittins_attained_buckets; ++attained)
            {
                for (size_t memory = 0; memory < gittins_memory_bands; ++memory)
                {
                    const double index = gittins_indices[attained][memory];
                    result[memory][attained] = index == 0
                        ? 0
                        : static_cast<UInt8>(1 + std::floor(std::log2(index / min_positive)));
                }
            }
            return result;
        }();
        return levels;
    }

    static size_t memoryBand(Int64 memory_usage)
    {
        constexpr UInt64 gib = 1ULL << 30;
        const UInt64 memory = memory_usage > 0 ? static_cast<UInt64>(memory_usage) : 0;
        if (memory >= 32 * gib) return 6;
        if (memory >= 16 * gib) return 5;
        if (memory >= 8 * gib) return 4;
        if (memory >= 4 * gib) return 3;
        if (memory >= 2 * gib) return 2;
        if (memory >= gib) return 1;
        return 0;
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    const CostUnit unit;
    const bool memory_conditioned;
    const bool normalize_indices;
    UInt64 next_seq = 0;
    Set requests;
};

/// `priority` — strict priority by the query's `workload_priority` setting. Lower value = higher
/// precedence, served first; the default `0` is the neutral baseline, a negative value raises the
/// query above it and a positive value lowers it. Ties (equal priority) fall back to FIFO by
/// arrival. Like any strict-priority scheme it can starve low-priority queries — `fair` is the
/// non-starving alternative.
class PriorityAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override
    {
        // Order by the query's `workload_priority`: lower value = higher precedence, so a negative
        // value sorts ahead of the default `0` and a positive value behind it. Ties break FIFO by
        // arrival sequence.
        chassert(request->scheduling.context); // set by the classifier that issued the link
        request->scheduling.priority = request->scheduling.context->priority;
        request->scheduling.key = {0.0, next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        auto it = requests.begin();
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(ResourceRequest::EnqueuedList & out) noexcept override
    {
        while (!requests.empty())
        {
            ResourceRequest & request = *requests.begin();
            requests.erase(requests.begin());
            out.push_back(request);
        }
    }

    void reset() noexcept override
    {
        chassert(requests.empty());
        next_seq = 0;
    }

    bool empty() const override { return requests.empty(); }

private:
    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            // Integer priority first (exact, lower value = higher priority), then the sequence for FIFO.
            if (lhs.scheduling.priority.value != rhs.scheduling.priority.value)
                return lhs.scheduling.priority.value < rhs.scheduling.priority.value;
            return lhs.scheduling.key.second < rhs.scheduling.key.second;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    UInt64 next_seq = 0;
    Set requests;
};

/*
 * Time-shared scheduler leaf that runs one of several pluggable scheduling algorithms, chosen by
 * the workload setting `scheduler` (default `fifo`). It is the time-shared
 * workload leaf. The leaf owns the cross-cutting concerns (mutex, budget via `ISchedulerQueue`,
 * `max_waiting_queries`, counters, activation) and delegates ordering to an `ISchedulingAlgorithm`.
 *
 * `setScheduler` switches the algorithm in place, moving all pending requests from the old one to
 * the new one, so a `CREATE OR REPLACE WORKLOAD` that changes `scheduler` neither rebuilds the
 * hierarchy nor invalidates the `ResourceLink` cached by classifiers, and loses no pending requests.
 * Moving requests (switch, eviction, purge) never allocates: these paths run on the scheduler thread
 * in response to SQL and must not fail halfway.
 */
class RequestQueue final : public ISchedulerQueue
{
    static constexpr Int64 default_max_queued = std::numeric_limits<Int64>::max();

public:
    explicit RequestQueue(
        EventQueue & event_queue_,
        const SchedulerNodeInfo & info_ = {},
        SchedulerAlgorithm algorithm_ = SchedulerAlgorithm::Fifo,
        CostUnit unit_ = CostUnit::IOByte,
        Int64 max_queued_ = default_max_queued)
        : ISchedulerQueue(event_queue_, info_)
        , fair_algorithm(unit_)
        , las_algorithm(unit_)
        , gittins_algorithm(unit_, false, true)
        , mem_gittins_algorithm(unit_, true, true)
        , gittins_raw_algorithm(unit_, false, false)
        , mem_gittins_raw_algorithm(unit_, true, false)
        , max_queued(max_queued_)
        , algorithm(algorithm_)
        , algo(&algorithmFor(algorithm_))
    {
    }

    ~RequestQueue() override
    {
        purgeQueue();
    }

    // Distinct type (parallels AllocationQueue's "allocation_queue"); the node's basename in the
    // hierarchy is "queue" (see WorkloadNodeTraits::makeQueue), which unifies the path naming.
    std::string_view getTypeName() const override { return "request_queue"; }

    void enqueueRequest(ResourceRequest * request) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue is about to be destructed");

        if (max_queued >= 0 && total_requests >= static_cast<size_t>(max_queued))
        {
            rejected_requests++;
            rejected_cost += request->cost;
            throw Exception(ErrorCodes::SERVER_OVERLOADED,
                "Workload limit `max_waiting_queries` has been reached: {} of {}", total_requests, max_queued);
        }

        assignFcfsSequence(*request);
        algo->push(request);
        queue_cost += request->cost;
        bool was_empty = total_requests == 0;
        total_requests++;
        SCHED_DBG("{} -- enqueue(cost={}, queued={})", basename, request->cost, total_requests);
        // `fcfs` and `if-fcfs` may have refused to dequeue while an earlier query held a quantum,
        // which deactivates the leaf with requests still queued. A new request (the earlier query
        // renewing, or the first request after it finished) has to wake the leaf again; `was_empty`
        // is false in that case.
        if (was_empty || algorithm == SchedulerAlgorithm::Fcfs || algorithm == SchedulerAlgorithm::IfFcfs)
            scheduleActivation();
    }

    /// Called from `ResourceRequest::finish()` on the consumer thread when a granted `fcfs` quantum ends.
    void releaseFcfsInflight(ResourceRequest & request)
    {
        std::lock_guard lock(mutex);
        const bool last_inflight = algorithm == SchedulerAlgorithm::IfFcfs
            ? if_fcfs_algorithm.releaseInflight(request.scheduling.state)
            : fcfs_algorithm.releaseInflight(request.scheduling.state);
        if (last_inflight && !algo->empty())
            scheduleActivation();
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        std::lock_guard lock(mutex);
        ResourceRequest * request = algo->pop();
        if (!request)
        {
            // `fcfs` and `if-fcfs` return null while a later query is queued behind an earlier
            // in-flight query. Leave those requests queued and report inactive so the scheduler waits.
            if (algo->empty())
                flushThroughputOnDeactivation();
            return {nullptr, false};
        }
        if (algorithm == SchedulerAlgorithm::Fcfs || algorithm == SchedulerAlgorithm::IfFcfs)
            request->scheduling.fcfs_leaf = this;
        // Charge attained service unconditionally: every request carries a valid per-query state,
        // stamped from its classifier link at enqueue. The declared cost is the estimate now that the
        // request is served; `finish()` corrects it to real cost later. Unread by `fifo`/`fcfs`/`priority`.
        // `srpt` observes the updated value when it refreshes keys before the next selection.
        request->scheduling.state->attained_cost.fetch_add(request->scheduling.cost, std::memory_order_relaxed);
        queue_cost -= request->cost;
        chassert(total_requests > 0);
        total_requests--;
        if (total_requests == 0)
        {
            busy_periods++;
            cancelActivation();
        }
        incrementDequeued(request->cost, total_requests > 0);
        SCHED_DBG("{} -- dequeue(cost={}, queued={})", getPath(), request->cost, total_requests);
        return {request, total_requests > 0};
    }

    bool cancelRequest(ResourceRequest * request) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            return false; // Any request should already be failed or executed
        if (!algo->erase(request))
            return false;
        queue_cost -= request->cost;
        chassert(total_requests > 0);
        total_requests--;
        canceled_requests++;
        canceled_cost += request->cost;
        if (total_requests == 0)
        {
            busy_periods++;
            cancelActivation();
        }
        return true;
    }

    void purgeQueue() override
    {
        // Collect requests to fail while holding the lock, but call failed() outside the lock
        // to avoid potential deadlock with CPULeaseAllocation::mutex (lock order inversion).
        ResourceRequest::EnqueuedList requests_to_fail;
        {
            std::lock_guard lock(mutex);
            is_not_usable = true;
            algo->pullAll(requests_to_fail);
            queue_cost = 0;
            total_requests = 0;
            cancelActivation();

            // Older hierarchy versions can retain this retired queue after its workload is destroyed.
            workload = nullptr;
        }
        failRequests(requests_to_fail, []
        {
            return std::make_exception_ptr(
                Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue with resource request is about to be destructed"));
        });
    }

    void updateQueueLimit(Int64 value) override
    {
        ResourceRequest::EnqueuedList requests_to_fail;
        {
            std::lock_guard lock(mutex);
            // `0` means "reject every waiting request" — a valid limit, as at construction and in
            // AllocationQueue; only a negative value is invalid.
            if (value < 0)
                throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Queue limit must not be negative, got: {}", value);
            max_queued = value;
            while (total_requests > static_cast<size_t>(max_queued))
            {
                ResourceRequest * request = algo->popWorst();
                chassert(request);
                queue_cost -= request->cost;
                total_requests--;
                rejected_requests++;
                rejected_cost += request->cost;
                requests_to_fail.push_back(*request);
            }
            if (total_requests == 0)
            {
                busy_periods++;
                cancelActivation();
            }
        }
        failRequests(requests_to_fail, []
        {
            return std::make_exception_ptr(
                Exception(ErrorCodes::SERVER_OVERLOADED, "Workload limit `max_waiting_queries` has been reached"));
        });
    }

    /// Switch the algorithm in place, moving all pending requests to the new one. Called when the
    /// workload `scheduler` setting changes; keeps the node, its `ResourceLink` and activation state.
    void setScheduler(SchedulerAlgorithm new_algorithm) noexcept
    {
        std::lock_guard lock(mutex);
        if (new_algorithm == algorithm)
            return;
        ResourceRequest::EnqueuedList pending;
        algo->pullAll(pending);
        algorithm = new_algorithm;
        algo = &algorithmFor(new_algorithm);
        algo->reset();
        // `reset` restarts the `fair` system virtual time at 0, so the moved queries' vruntime restarts
        // too; a stale projection would be counted twice. `attained_cost` is real service and is kept.
        if (new_algorithm == SchedulerAlgorithm::Fair)
            for (ResourceRequest & request : pending)
                request.scheduling.state->fair.vruntime = 0.0;
        // Same for `fair-max`: `reset` restarts the system value at 0, so a moved query must be
        // initialized again instead of keeping a projection from the previous algorithm instance.
        if (new_algorithm == SchedulerAlgorithm::FairMax)
            for (ResourceRequest & request : pending)
            {
                request.scheduling.state->fair_max.vruntime = 0.0;
                request.scheduling.state->fair_max.initialized = false;
            }
        // `fair-tier-5` and `fair-tier-10` likewise restart from an empty active set, so a moved
        // query must join again instead of keeping a base tier from the previous algorithm instance.
        if (new_algorithm == SchedulerAlgorithm::FairTier5 || new_algorithm == SchedulerAlgorithm::FairTier10)
            for (ResourceRequest & request : pending)
            {
                request.scheduling.state->fair_tier.base_tier = 0.0;
                request.scheduling.state->fair_tier.initialized = false;
            }
        while (!pending.empty())
        {
            ResourceRequest & request = pending.front();
            pending.pop_front(); // `fifo` links the request through the same hook
            assignFcfsSequence(request);
            algo->push(&request);
        }
    }

    SchedulerAlgorithm getScheduler() const
    {
        std::lock_guard lock(mutex);
        return algorithm;
    }

    bool isActive() override
    {
        std::lock_guard lock(mutex);
        return total_requests > 0;
    }

    size_t activeChildren() override { return 0; }

    void activateChild(ITimeSharedNode &) override
    {
        chassert(false); // queue cannot have children
    }

    void attachChild(const SchedulerNodePtr &) override
    {
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Cannot add child to leaf scheduler queue: {}", getPath());
    }

    void removeChild(ISchedulerNode *) override {}

    ISchedulerNode * getChild(const String &) override { return nullptr; }

    std::pair<UInt64, Int64> getQueueLengthAndCost() override
    {
        std::lock_guard lock(mutex);
        return {total_requests, queue_cost};
    }

private:
    ISchedulingAlgorithm & algorithmFor(SchedulerAlgorithm algorithm_) noexcept
    {
        switch (algorithm_)
        {
            case SchedulerAlgorithm::Fifo: return fifo_algorithm;
            case SchedulerAlgorithm::Fcfs: return fcfs_algorithm;
            case SchedulerAlgorithm::IfFcfs: return if_fcfs_algorithm;
            case SchedulerAlgorithm::Fair: return fair_algorithm;
            case SchedulerAlgorithm::Las: return las_algorithm;
            case SchedulerAlgorithm::Gittins: return gittins_algorithm;
            case SchedulerAlgorithm::MemGittins: return mem_gittins_algorithm;
            case SchedulerAlgorithm::GittinsRaw: return gittins_raw_algorithm;
            case SchedulerAlgorithm::MemGittinsRaw: return mem_gittins_raw_algorithm;
            case SchedulerAlgorithm::Priority: return priority_algorithm;
            case SchedulerAlgorithm::Srpt: return srpt_algorithm;
            case SchedulerAlgorithm::FairMax: return fair_max_algorithm;
            case SchedulerAlgorithm::FairTier5: return fair_tier_5_algorithm;
            case SchedulerAlgorithm::FairTier10: return fair_tier_10_algorithm;
        }
        UNREACHABLE();
    }

    /// Unlink each request, then fail it: its owner may destroy it in `failed`. If building the
    /// exception throws, that error fails the requests instead, so none is left pending.
    template <typename MakeException>
    static void failRequests(ResourceRequest::EnqueuedList & requests, const MakeException & make_exception)
    {
        if (requests.empty())
            return;
        std::exception_ptr exception;
        try
        {
            exception = make_exception();
        }
        catch (...)
        {
            exception = std::current_exception();
        }
        while (!requests.empty())
        {
            ResourceRequest & request = requests.front();
            requests.pop_front();
            request.failed(exception);
        }
    }

    /// First accepted request of a query on this leaf takes the next sequence. Later requests of that
    /// query reuse it. The counter is not reset when the algorithm changes, so a query keeps its place.
    void assignFcfsSequence(ResourceRequest & request)
    {
        ResourceQueryState & state = *request.scheduling.state;
        if (state.fcfs_sequence_assigned)
            return;
        state.fcfs_sequence = next_fcfs_sequence++;
        state.fcfs_sequence_assigned = true;
    }

    mutable std::mutex mutex;
    UInt64 next_fcfs_sequence = 0;
    /// One instance of each algorithm, so switching never allocates; `algo` points at the active one.
    FifoAlgorithm fifo_algorithm;
    FcfsAlgorithm fcfs_algorithm;
    IfFcfsAlgorithm if_fcfs_algorithm;
    FairAlgorithm fair_algorithm;
    LasAlgorithm las_algorithm;
    GittinsAlgorithm gittins_algorithm;
    GittinsAlgorithm mem_gittins_algorithm;
    GittinsAlgorithm gittins_raw_algorithm;
    GittinsAlgorithm mem_gittins_raw_algorithm;
    PriorityAlgorithm priority_algorithm;
    SrptAlgorithm srpt_algorithm;
    FairMaxAlgorithm fair_max_algorithm;
    FairTierAlgorithm fair_tier_5_algorithm{FairTierAlgorithm::tier_5_cost};
    FairTierAlgorithm fair_tier_10_algorithm{FairTierAlgorithm::tier_10_cost};
    Int64 max_queued;
    SchedulerAlgorithm algorithm;
    ISchedulingAlgorithm * algo;
    ResourceCost queue_cost = 0;
    size_t total_requests = 0;
    bool is_not_usable = false;
};

}
