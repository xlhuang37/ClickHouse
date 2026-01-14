#pragma once

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/Priority.h>
#include <Common/EventRateMeter.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>
#include <base/types.h>

#include <Common/Scheduler/ResourceRequest.h>
#include <Common/ProfileEvents.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include <boost/noncopyable.hpp>
#include <boost/intrusive/list.hpp>

#include <chrono>
#include <deque>
#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>

namespace ProfileEvents
{
    extern const Event SchedulerNodeUpdate;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

class ISchedulerNode;
class EventQueue;
using EventId = UInt64;

inline const Poco::Util::AbstractConfiguration & emptyConfig()
{
    static Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration();
    return *config;
}

struct WorkloadRuntimeStats
{
    std::atomic<uint32_t> running_queries{0};
    // std::atomic<double>  runtime_factor{1.0};  // multiplier on top of base weight
 };

/*
 * Info read and write for scheduling purposes by parent
 */
struct SchedulerNodeInfo
{
    double weight = 1.0; /// Weight of this node among it's siblings
    Priority priority; /// Priority of this node among it's siblings (lower value means higher priority)
    Int64 queue_size = std::numeric_limits<Int64>::max(); /// Size of a workload queue
    std::shared_ptr<WorkloadRuntimeStats> runtime_stats;

    /// Speedup vs. core count (index i = cores-1).
    ///  - Almost linear until 32 cores.
    ///  - After 32 cores, still increases but with smaller per-core gain.
    const std::array<double, 64> parallel_speedup = {
        0.0000,   // 0 cores
        1.0000,   // 1 core
        1.9836,   // 2 cores
        2.9424,   // 3 cores
        3.7905,   // 4 cores
        4.6265,   // 5 cores
        5.4625,   // 6 cores
        6.2985,   // 7 cores
        7.1345,   // 8 cores
        7.9705,   // 9 cores
        8.8065,   // 10 cores
        9.6425,   // 11 cores
        10.4785,  // 12 cores
        11.3138,  // 13 cores
        12.1491,  // 14 cores
        12.9844,  // 15 cores
        13.7840,  // 16 cores
        14.5836,  // 17 cores
        15.3832,  // 18 cores
        16.1828,  // 19 cores
        16.9294,  // 20 cores
        17.6760,  // 21 cores
        18.4226,  // 22 cores
        19.1692,  // 23 cores
        19.9158,  // 24 cores
        20.6624,  // 25 cores
        21.4090,  // 26 cores
        22.1556,  // 27 cores
        22.8429,  // 28 cores
        23.5302,  // 29 cores
        24.2175,  // 30 cores
        24.7708,  // 31 cores
        25.3241,  // 32 cores
        25.8774,  // 33 cores
        26.4307,  // 34 cores
        26.9059,  // 35 cores
        27.3811,  // 36 cores
        27.8563,  // 37 cores
        28.3315,  // 38 cores
        28.8067,  // 39 cores
        29.2320,  // 40 cores
        29.6573,  // 41 cores
        30.0826,  // 42 cores
        30.4246,  // 43 cores
        30.7666,  // 44 cores
        30.9294,  // 45 cores
        31.0922,  // 46 cores
        // Flat from here (repeat last value)
        31.0922, 31.0922, 31.0922, 31.0922, 31.0922, 31.0922, 31.0922, 31.0922, 31.0922, 31.0922,
        31.0922, 31.0922, 31.0922, 31.0922, 31.0922, 31.0922, 31.0922
    };
    
    /// Speedup vs. core count for almost-serial workload:
    ///  - Almost linear up to 2 cores.
    ///  - Completely flat after 2 cores.
    const std::array<double, 64> nonparallel_speedup = [] {
        std::array<double, 64> s{};
        // Your measured values
        s[0] = 0.0000;
        s[1] = 1.0000;
        s[2] = 1.9136;
        // Fill remaining with last value
        for (size_t i = 3; i < s.size(); ++i) {
            s[i] = 1.9136;
        }
        return s;
    }();

    const std::array<double, 64> * active_speedup = nullptr;

    /// Arbitrary data accessed/stored by parent
    union {
        size_t idx;
        void * ptr;
    } parent;

    SchedulerNodeInfo()
        : runtime_stats(std::make_shared<WorkloadRuntimeStats>())
    {}

    explicit SchedulerNodeInfo(double weight_, Priority priority_ = {})
        : runtime_stats(std::make_shared<WorkloadRuntimeStats>())
    {
        setWeight(weight_);
        setPriority(priority_);
    }

    explicit SchedulerNodeInfo(const Poco::Util::AbstractConfiguration & config, const String & config_prefix = {}) 
        : runtime_stats(std::make_shared<WorkloadRuntimeStats>())
    {
        setWeight(config.getDouble(config_prefix + ".weight", weight));
        setPriority(config.getInt64(config_prefix + ".priority", priority));
        setQueueSize(config.getInt64(config_prefix + ".queue_size", queue_size));
    }

    void updateRuntimeStatQueryStart() {
        ProfileEvents::increment(ProfileEvents::SchedulerNodeUpdate);
        runtime_stats->running_queries.fetch_add(1, std::memory_order_relaxed);
    }

    void updateRuntimeStatQueryEnd() {
        ProfileEvents::increment(ProfileEvents::SchedulerNodeUpdate);
        runtime_stats->running_queries.fetch_sub(1, std::memory_order_relaxed);
    }

    void setWeight(double value)
    {
        if (value <= 0 || !isfinite(value))
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Zero, negative and non-finite node weights are not allowed: {}",
                value);
        weight = value;
    }

    void setQueueSize(Int64 value)
    {
        if (value <= 0)
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Workload setting `max_waiting_queries` value must be positive, got: {}",
                value);
        queue_size = value;
    }

    void setPriority(Int64 value)
    {
        priority.value = value;
    }

    void setPriority(Priority value)
    {
        priority = value;
    }

    // To check if configuration update required
    bool equals(const SchedulerNodeInfo & o) const
    {
        // `parent` data is not compared intentionally (it is not part of configuration settings)
        return weight == o.weight && priority == o.priority;
    }

    void useParallelSpeedupProfile()
    {
        active_speedup = &parallel_speedup;
    }

    void useNonParallelSpeedupProfile()
    {
        active_speedup = &nonparallel_speedup;
    }
};


/*
 * Node of hierarchy for scheduling requests for resource. Base class for all
 * kinds of scheduling elements (queues, policies, constraints and schedulers).
 *
 * Root node is a scheduler, which has it's thread to dequeue requests,
 * execute requests (see ResourceRequest) and process events in a thread-safe manner.
 * Immediate children of the scheduler represent independent resources.
 * Each resource has it's own hierarchy to achieve required scheduling policies.
 * Non-leaf nodes do not hold requests, but keep scheduling state
 * (e.g. consumption history, amount of in-flight requests, etc).
 * Leafs of hierarchy are queues capable of holding pending requests.
 *
 *        scheduler         (SchedulerRoot)
 *         /     \
 *  constraint  constraint  (SemaphoreConstraint)
 *      |           |
 *   policy      policy     (PriorityPolicy)
 *   /    \      /    \
 *  q1    q2    q3    q4    (FifoQueue)
 *
 * Dequeueing request from an inner node will dequeue request from one of active leaf-queues in its subtree.
 * Node is considered to be active iff:
 *  - it has at least one pending request in one of leaves of it's subtree;
 *  - and enforced constraints, if any, are satisfied
 *    (e.g. amount of concurrent requests is not greater than some number).
 *
 * All methods must be called only from scheduler thread for thread-safety.
 */
class ISchedulerNode : public boost::intrusive::list_base_hook<>, private boost::noncopyable
{
public:
    explicit ISchedulerNode(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : event_queue(event_queue_)
        , info(config, config_prefix)
    {}

    ISchedulerNode(EventQueue * event_queue_, const SchedulerNodeInfo & info_)
        : event_queue(event_queue_)
        , info(info_)
    {}

    virtual ~ISchedulerNode();

    virtual const String & getTypeName() const = 0;

    /// Checks if two nodes configuration is equal
    virtual bool equals(ISchedulerNode * other)
    {
        return info.equals(other->info);
    }

    /// Attach new child
    virtual void attachChild(const std::shared_ptr<ISchedulerNode> & child) = 0;

    /// Detach child
    /// NOTE: child might be destroyed if the only reference was stored in parent
    virtual void removeChild(ISchedulerNode * child) = 0;

    /// Get attached child by name (for tests only)
    virtual ISchedulerNode * getChild(const String & child_name) = 0;

    /// Activation of child due to the first pending request
    /// Should be called on leaf node (i.e. queue) to propagate activation signal through chain to the root
    virtual void activateChild(ISchedulerNode * child) = 0;

    /// Returns true iff node is active
    virtual bool isActive() = 0;

    /// Returns number of active children (for introspection only).
    virtual size_t activeChildren() = 0;

    /// Returns the first request to be executed as the first component of resulting pair.
    /// The second pair component is `true` iff node is still active after dequeueing.
    virtual std::pair<ResourceRequest *, bool> dequeueRequest() = 0;

    /// Returns full path string using names of every parent
    String getPath() const
    {
        String result;
        const ISchedulerNode * ptr = this;
        while (ptr->parent)
        {
            result = "/" + ptr->basename + result;
            ptr = ptr->parent;
        }
        return result.empty() ? "/" : result;
    }

    /// Attach to a parent (used by attachChild)
    void setParent(ISchedulerNode * parent_);

protected:
    /// Notify parents about the first pending request or constraint becoming satisfied.
    /// Postponed to be handled in scheduler thread, so it is intended to be called from outside.
    void scheduleActivation();

    /// Helper for introspection metrics
    void incrementDequeued(ResourceCost cost)
    {
        dequeued_requests++;
        dequeued_cost += cost;
        throughput.add(static_cast<double>(clock_gettime_ns())/1e9, cost);
    }

public:
    EventQueue * const event_queue;
    String basename;
    SchedulerNodeInfo info;
    ISchedulerNode * parent = nullptr;
    EventId activation_event_id = 0; // Valid for `ISchedulerNode` placed in EventQueue::activations

    /// Introspection
    std::atomic<UInt64> dequeued_requests{0};
    std::atomic<UInt64> canceled_requests{0};
    std::atomic<ResourceCost> dequeued_cost{0};
    std::atomic<ResourceCost> canceled_cost{0};
    std::atomic<UInt64> busy_periods{0};

    /// Average dequeued_cost per second
    /// WARNING: Should only be accessed from the scheduler thread, so that locking is not required
    EventRateMeter throughput{static_cast<double>(clock_gettime_ns())/1e9, 2, 1};
};

using SchedulerNodePtr = std::shared_ptr<ISchedulerNode>;

/*
 * Simple waitable thread-safe FIFO task queue.
 * Intended to hold postponed events for later handling (usually by scheduler thread).
 */
class EventQueue
{
public:
    using Task = std::function<void()>;

    static constexpr EventId not_postponed = 0;

    using TimePoint = std::chrono::system_clock::time_point;
    using Duration = std::chrono::system_clock::duration;

    struct Event
    {
        const EventId event_id;
        Task task;

        Event(EventId event_id_, Task && task_)
            : event_id(event_id_)
            , task(std::move(task_))
        {}
    };

    struct Postponed
    {
        TimePoint key;
        EventId event_id; // for canceling
        std::unique_ptr<Task> task;

        Postponed(TimePoint key_, EventId event_id_, Task && task_)
            : key(key_)
            , event_id(event_id_)
            , task(std::make_unique<Task>(std::move(task_)))
        {}

        bool operator<(const Postponed & rhs) const
        {
            return std::tie(key, event_id) > std::tie(rhs.key, rhs.event_id); // reversed for min-heap
        }
    };

    /// Add an `event` to be processed after `until` time point.
    /// Returns a unique event id for canceling.
    [[nodiscard]] EventId postpone(TimePoint until, Task && task)
    {
        std::unique_lock lock{mutex};
        if (postponed.empty() || until < postponed.front().key)
            pending.notify_one();
        auto event_id = ++last_event_id;
        postponed.emplace_back(until, event_id, std::move(task));
        std::push_heap(postponed.begin(), postponed.end());
        return event_id;
    }

    /// Cancel a postponed event using its unique id.
    /// NOTE: Only postponed events can be canceled.
    /// NOTE: If you need to cancel enqueued event, consider doing your actions inside another enqueued
    /// NOTE: event instead. This ensures that all previous events are processed.
    bool cancelPostponed(EventId postponed_event_id)
    {
        if (postponed_event_id == not_postponed)
            return false;
        std::unique_lock lock{mutex};
        for (auto i = postponed.begin(), e = postponed.end(); i != e; ++i)
        {
            if (i->event_id == postponed_event_id)
            {
                postponed.erase(i);
                // It is O(n), but we do not expect neither big heaps nor frequent cancels. So it is fine.
                std::make_heap(postponed.begin(), postponed.end());
                return true;
            }
        }
        return false;
    }

    /// Add an `event` for immediate processing
    void enqueue(Task && task)
    {
        std::unique_lock lock{mutex};
        bool was_empty = events.empty() && activations.empty();
        auto event_id = ++last_event_id;
        events.emplace_back(event_id, std::move(task));
        if (was_empty)
            pending.notify_one();
    }

    /// Add an activation `event` for immediate processing. Activations use a separate queue for performance reasons.
    void enqueueActivation(ISchedulerNode * node)
    {
        std::unique_lock lock{mutex};
        bool was_empty = events.empty() && activations.empty();
        node->activation_event_id = ++last_event_id;
        activations.push_back(*node);
        if (was_empty)
            pending.notify_one();
    }

    /// Removes an activation from queue
    void cancelActivation(ISchedulerNode * node)
    {
        std::unique_lock lock{mutex};
        if (node->is_linked())
            activations.erase(activations.iterator_to(*node));
        node->activation_event_id = 0;
    }

    /// Process single event if it exists
    /// Note that postponing constraint are ignored, use it to empty the queue including postponed events on shutdown
    /// Returns `true` iff event has been processed
    bool forceProcess()
    {
        std::unique_lock lock{mutex};
        if (!events.empty() || !activations.empty())
        {
            processQueue(std::move(lock));
            return true;
        }
        if (!postponed.empty())
        {
            processPostponed(std::move(lock));
            return true;
        }
        return false;
    }

    /// Process single event if it exists and meets postponing constraint
    /// Returns `true` iff event has been processed
    bool tryProcess()
    {
        std::unique_lock lock{mutex};
        if (!events.empty() || !activations.empty())
        {
            processQueue(std::move(lock));
            return true;
        }
        if (postponed.empty())
            return false;

        if (postponed.front().key <= now())
        {
            processPostponed(std::move(lock));
            return true;
        }
        return false;
    }

    /// Wait for single event (if not available) and process it
    void process()
    {
        std::unique_lock lock{mutex};
        while (true)
        {
            if (!events.empty() || !activations.empty())
            {
                processQueue(std::move(lock));
                return;
            }
            if (postponed.empty())
            {
                wait(lock);
            }
            else
            {
                if (postponed.front().key <= now())
                {
                    processPostponed(std::move(lock));
                    return;
                }

                waitUntil(lock, postponed.front().key);
            }
        }
    }

    TimePoint now()
    {
        auto result = manual_time.load();
        if (likely(result == TimePoint()))
            return std::chrono::system_clock::now();
        return result;
    }

    /// For testing only
    void setManualTime(TimePoint value)
    {
        std::unique_lock lock{mutex};
        manual_time.store(value);
        pending.notify_one();
    }

    /// For testing only
    void advanceManualTime(Duration elapsed)
    {
        std::unique_lock lock{mutex};
        manual_time.store(manual_time.load() + elapsed);
        pending.notify_one();
    }

private:
    void wait(std::unique_lock<std::mutex> & lock)
    {
        pending.wait(lock);
    }

    void waitUntil(std::unique_lock<std::mutex> & lock, TimePoint t)
    {
        if (likely(manual_time.load() == TimePoint()))
            pending.wait_until(lock, t);
        else
            pending.wait(lock);
    }

    void processQueue(std::unique_lock<std::mutex> && lock)
    {
        if (events.empty())
        {
            processActivation(std::move(lock));
            return;
        }
        if (activations.empty())
        {
            processEvent(std::move(lock));
            return;
        }
        if (activations.front().activation_event_id < events.front().event_id)
            processActivation(std::move(lock));
        else
            processEvent(std::move(lock));
    }

    void processActivation(std::unique_lock<std::mutex> && lock)
    {
        ISchedulerNode * node = &activations.front();
        activations.pop_front();
        node->activation_event_id = 0;
        lock.unlock(); // do not hold queue mutex while processing events
        node->parent->activateChild(node);
    }

    void processEvent(std::unique_lock<std::mutex> && lock)
    {
        Task task = std::move(events.front().task);
        events.pop_front();
        lock.unlock(); // do not hold queue mutex while processing events
        task();
    }

    void processPostponed(std::unique_lock<std::mutex> && lock)
    {
        Task task = std::move(*postponed.front().task);
        std::pop_heap(postponed.begin(), postponed.end());
        postponed.pop_back();
        lock.unlock(); // do not hold queue mutex while processing events
        task();
    }

    std::mutex mutex;
    std::condition_variable pending;

    // `events` and `activations` logically represent one ordered queue. To preserve the common order we use `EventId`
    // Activations are stored in a separate queue for performance reasons (mostly to avoid any allocations)
    std::deque<Event> events;
    boost::intrusive::list<ISchedulerNode> activations;

    std::vector<Postponed> postponed;
    EventId last_event_id = 0;

    std::atomic<TimePoint> manual_time{TimePoint()}; // for tests only
};

inline ISchedulerNode::~ISchedulerNode()
{
    // Make sure there is no dangling reference in activations queue
    event_queue->cancelActivation(this);
}

inline void ISchedulerNode::setParent(ISchedulerNode * parent_)
{
    parent = parent_;
    // Avoid activation of a detached node
    if (parent == nullptr)
        event_queue->cancelActivation(this);
}

inline void ISchedulerNode::scheduleActivation()
{
    if (likely(parent))
    {
        // The same as `enqueue([this] { parent->activateChild(this); });` but faster
        event_queue->enqueueActivation(this);
    }
}

}
