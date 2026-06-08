#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/ISchedulerPriorityQueue.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/Nodes/MultiLevelFeedbackQueue.h>
#include <Common/Exception.h>
#include <Common/OSThreadRealtime.h>
#include <Common/ProfileEvents.h>
#include <Common/RealTimeSlotPool.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/logger_useful.h>

#include <atomic>
#include <utility>

#if 0
#define LOG_EVENT(X) LOG_TRACE(log, "{}:{} ({}) allocated={} granted={} running={} L:{} P:{} <{}/{}> e:{}", \
    lease_id, settings.workload, #X, allocated, granted, threads.running_count, formatBitset(threads.leased), \
    formatBitset(threads.preempted), consumed_ns, requested_ns, requests.hasEnqueued())
namespace
{
    std::string formatBitset(const boost::dynamic_bitset<> & bits)
    {
        std::string result;
        result.reserve(bits.size());
        for (size_t i = 0; i < bits.size(); ++i)
            result += bits[i] ? '1' : '0';
        return result;
    }
}

#else
#define LOG_EVENT(X) void(0)
#endif

namespace ProfileEvents
{
    extern const Event ConcurrencyControlWaitMicroseconds;
    extern const Event ConcurrencyControlPreemptedMicroseconds;
    extern const Event ConcurrencyControlSlotsAcquired;
    extern const Event ConcurrencyControlPreemptions;
    extern const Event ConcurrencyControlUpscales;
    extern const Event ConcurrencyControlDownscales;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlScheduled;
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlPreempted;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
    extern const int RESOURCE_ACCESS_DENIED;
}

std::atomic<size_t> CPULeaseAllocation::lease_counter{0};

CPULeaseAllocation::Lease::Lease(CPULeaseAllocationPtr && parent_, size_t slot_id_)
    : ISlotLease(slot_id_)
    , parent(std::move(parent_))
{}

CPULeaseAllocation::Lease::~Lease()
{
    if (parent)
    {
        std::optional<OpenTelemetry::SpanHolder> span;
        if (parent->settings.trace_cpu_scheduling)
        {
            span.emplace("CPU_LEASE_STOP");
            span->addAttribute("workload", parent->settings.workload);
            span->addAttribute("lease_id", parent->getLeaseId());
            span->addAttribute("thread_number", slot_id);
        }
        parent->release(*this);
    }
}

void CPULeaseAllocation::Lease::startConsumption()
{
    last_report_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    if (parent && parent->settings.trace_cpu_scheduling)
    {
        OpenTelemetry::SpanHolder span("CPU_LEASE_START");
        span.addAttribute("workload", parent->settings.workload);
        span.addAttribute("lease_id", parent->getLeaseId());
        span.addAttribute("thread_number", slot_id);
    }
}

bool CPULeaseAllocation::Lease::renew()
{
    if (parent)
        return parent->renew(*this);
    else
        return false;
}

void CPULeaseAllocation::Lease::relinquishRealtime()
{
    if (parent)
        parent->relinquishRealtime(*this);
}

void CPULeaseAllocation::Lease::reset()
{
    if (parent->settings.trace_cpu_scheduling)
    {
        OpenTelemetry::SpanHolder span("CPU_LEASE_DOWNSCALED");
        span.addAttribute("workload", parent->settings.workload);
        span.addAttribute("lease_id", parent->getLeaseId());
        span.addAttribute("thread_number", slot_id);
    }
    parent.reset();
}

CPULeaseAllocation::RequestChain::RequestChain(CPULeaseAllocation * lease, size_t max_threads_, ResourceLink master_link_, ResourceLink worker_link_)
    : master_link(master_link_)
    , worker_link(worker_link_)
    , requests(max_threads_) // NOTE: it should not be reallocated after initialization because we use raw pointers and iterators
    , head(requests.begin())
    , tail(requests.begin())
{
    chassert(max_threads_ > 0);
    for (Request & request : requests)
        request.lease = lease;
}

void CPULeaseAllocation::RequestChain::finish()
{
    tail->finish();
    if (tail->is_master_slot)
        request_master_slot = true;
    ++tail;
    if (tail == requests.end())
        tail = requests.begin();
}

void CPULeaseAllocation::RequestChain::granted()
{
    ++head;
    if (head == requests.end())
        head = requests.begin();
}

CPULeaseAllocation::RequestChain::EnqueueResult CPULeaseAllocation::RequestChain::enqueue(
    ResourceCost cost,
    ResourceCost requested_ns_,
    Priority priority,
    bool throttle_non_master)
{
    chassert(!enqueued);

    // Do not throttle the master-slot request: it must keep progressing even when
    // worker parallelism is capped by the dynamic tasks-based limit.
    if (throttle_non_master && !request_master_slot)
        return EnqueueResult::Throttled;

    head->reset(cost);
    head->is_master_slot = std::exchange(request_master_slot, false);
    head->max_consumed = requested_ns_;  // Lease expires if we consume what we requested

    if (auto * queue = head->is_master_slot ? master_link.queue : worker_link.queue)
    {
        head->is_noncompeting = false;
        // We do not use enqueueRequestUsingBudget() because it redistributes resource between requests in the queue (which might be from different queries).
        // Instead we do budgeting for every query independently for better fairness.
        // All queues created via UnifiedSchedulerNode are MultiLevelFeedbackQueue, so the downcast
        // is expected to always succeed. The chassert guards against someone later wiring a
        // non-priority queue here.
        auto * pqueue = dynamic_cast<ISchedulerPriorityQueue *>(queue);
        chassert(pqueue);
        pqueue->enqueueRequest(&*head, priority);
        enqueued = true;
        return EnqueueResult::Enqueued; // Request is enqueued to the scheduler queue, we will wait for it to be granted
    }
    else // noncompeting slot - provide immediately for free
    {
        head->is_noncompeting = true;
        return EnqueueResult::NonCompeting; // No need to enqueue, we will grant it immediately
    }
}

void CPULeaseAllocation::RequestChain::reprioritize(Priority priority)
{
    if (!enqueued)
        return; // Nothing is waiting in the scheduler queue, the level will be picked at the next enqueue().

    // The currently enqueued request is `&*head` (head is advanced only on grant). It lives in
    // the master or worker queue depending on its slot kind. Move it in place to the new level.
    auto * queue = head->is_master_slot ? master_link.queue : worker_link.queue;
    chassert(queue);
    auto * pqueue = dynamic_cast<ISchedulerPriorityQueue *>(queue);
    chassert(pqueue);
    pqueue->reprioritizeRequest(&*head, priority);
}

void CPULeaseAllocation::RequestChain::cancel(std::unique_lock<std::mutex> & lock)
{
    if (enqueued)
    {
        auto * queue = head->is_master_slot ? master_link.queue : worker_link.queue;
        chassert(queue);
        bool canceled = queue->cancelRequest(&*head);
        if (!canceled) // Request is currently processed by the scheduler thread, we have to wait
        {
            wait_cancel = true;
            cancel_cv.wait(lock, [this] { return !enqueued; });
            wait_cancel = false;
        }
        else
            enqueued = false;
    }
}

void CPULeaseAllocation::RequestChain::scheduled()
{
    // It is either executed (granted) or failed, but it is not enqueued anymore
    enqueued = false;

    // Notify cancel() that pending request is detached from the scheduler
    if (wait_cancel)
        cancel_cv.notify_one();
}

CPULeaseAllocation::CPULeaseAllocation(SlotCount max_threads_, ResourceLink master_link_, ResourceLink worker_link_, CPULeaseSettings settings_)
    : max_threads(max_threads_)
    , settings(std::move(settings_))
    , log(getLogger("CPULeaseAllocation"))
    , threads(max_threads)
    , requests(this, max_threads, master_link_, worker_link_)
    , acquired_increment(CurrentMetrics::ConcurrencyControlAcquired, 0)
    , scheduled_increment(CurrentMetrics::ConcurrencyControlScheduled, 0)
    , lease_id(lease_counter.fetch_add(1, std::memory_order_relaxed))
{
    // Capture query-level counters (ThreadGroup) that outlive all worker threads.
    // Cannot use CurrentThread::getProfileEvents() in schedule() — it returns the calling
    // thread's counters, which may be destroyed before the timer is flushed (UAF).
    wait_thread_group = CurrentThread::getGroup();
    if (wait_thread_group)
        wait_counters = &wait_thread_group->performance_counters;

    std::unique_lock lock{mutex};
    if (!schedule(lock))
        grantImpl(lock);
}

CPULeaseAllocation::~CPULeaseAllocation()
{
    free();
}

void CPULeaseAllocation::free()
{
    std::unique_lock lock{mutex};

    if (shutdown)
        return;

    shutdown = true;
    acquirable.store(false, std::memory_order_relaxed);
    wait_timer.reset();

    // Return the process-wide RT permit (if held) to the pool and leave real-time mode. The master
    // thread still running SCHED_FIFO reverts to the default policy itself on its next renew() (which
    // observes `shutdown`); we do not touch its OS policy here because it must be done on that thread.
    realtime_mode = false;
    if (realtime_permit_held)
    {
        RealTimeSlotPool::instance().release();
        realtime_permit_held = false;
    }

    // Wake up all preempted threads
    while (true)
    {
        if (size_t thread_num = threads.preempted.find_first(); thread_num != boost::dynamic_bitset<>::npos)
            resetPreempted(thread_num);
        else
            break; // No preempted threads, we are done
    }

    // Properly cancel pending resource request (if any)
    requests.cancel(lock);

    // Finish all resource requests in consumption state
    while (allocated > 0)
    {
        --allocated;
        --granted;
        requests.finish();
        LOG_EVENT(S);
    }
}

[[nodiscard]] AcquiredSlotPtr CPULeaseAllocation::tryAcquire()
{
    if (!acquirable.load(std::memory_order_relaxed))
        return {}; // shortcut to avoid unnecessary mutex locking

    std::unique_lock lock{mutex};
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
    if (granted > 0)
        return acquireImpl(lock);
    return {};
}

[[nodiscard]] AcquiredSlotPtr CPULeaseAllocation::acquire()
{
    std::unique_lock lock{mutex};
    if (threads.leased.count() == max_threads)
        return {}; // Max number of threads already acquired
    return acquireImpl(lock);
}

AcquiredSlotPtr CPULeaseAllocation::acquireImpl(std::unique_lock<std::mutex> &)
{
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired);
    acquired_increment.add();
    return AcquiredSlotPtr(new Lease(std::static_pointer_cast<CPULeaseAllocation>(shared_from_this()), upscale()));
}

size_t CPULeaseAllocation::upscale()
{
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlUpscales);

    // New thread take one granted slot
    --granted; // Might became negative, but it is ok because we are going to allocate a slot later
    if (granted <= 0 && !exception)
        acquirable.store(false, std::memory_order_relaxed);

    for (size_t thread_num = 0; thread_num < max_threads; ++thread_num)
    {
        if (!threads.leased[thread_num])
        {
            threads.leased.set(thread_num);
            chassert(!threads.preempted[thread_num]);
            // Update fields about running threads
            if (++threads.running_count == 1)
                threads.last_running = thread_num;
            else
                threads.last_running = std::max(threads.last_running, thread_num);
            LOG_EVENT(U);
            return thread_num;
        }
    }
    chassert(false);
    return max_threads;
}

void CPULeaseAllocation::downscale(size_t thread_num, bool shutdown_)
{
    if (!shutdown_)
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlDownscales);

    chassert(threads.leased[thread_num]);
    threads.leased.reset(thread_num);

    if (threads.preempted[thread_num])
        threads.preempted.reset(thread_num);
    else
    {
        // Update fields about running threads
        --threads.running_count;
        if (threads.last_running == thread_num)
        {
            while (threads.last_running-- > 0)
            {
                if (threads.leased[threads.last_running] && !threads.preempted[threads.last_running])
                    break;
            }
        }

        // We have stopped a running thread that held an acquired slot, which becomes granted
        ++granted;
        if (granted > 0 && !shutdown)
            acquirable.store(true, std::memory_order_relaxed);
    }
    LOG_EVENT(D);
}

void CPULeaseAllocation::setPreempted(size_t thread_num)
{
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlPreemptions);

    // Mark the thread as preempted
    chassert(threads.leased[thread_num]);
    chassert(!threads.preempted[thread_num]);
    threads.preempted.set(thread_num);

    // Update fields about running threads
    --threads.running_count;
    if (threads.last_running == thread_num)
    {
        while (threads.last_running-- > 0)
        {
            if (threads.leased[threads.last_running] && !threads.preempted[threads.last_running])
                break;
        }
    }

    // Preempted thread does not hold the slot, and it becomes granted
    // Note that at this point granted is almost always negative (see consume()), so it would not lead to acquiring more threads
    ++granted;
    if (granted > 0 && !shutdown)
        acquirable.store(true, std::memory_order_relaxed);
    LOG_EVENT(P);
}

void CPULeaseAllocation::resetPreempted(size_t thread_num)
{
    // When resumed thread acquires one granted slot
    --granted;
    if (granted <= 0 && !exception)
        acquirable.store(false, std::memory_order_relaxed);

    // Mark the thread as not preempted
    chassert(threads.leased[thread_num]);
    threads.preempted.reset(thread_num);

    // Update fields about running threads
    if (++threads.running_count == 1)
        threads.last_running = thread_num;
    else
        threads.last_running = std::max(threads.last_running, thread_num);

    // Wake the thread
    threads.wake[thread_num].notify_one();
    LOG_EVENT(R);
}

void CPULeaseAllocation::failed(const std::exception_ptr & ptr)
{
    // This code runs in the scheduler thread, so we have to keep it fast and simple
    std::unique_lock lock{mutex};
    requests.scheduled();
    scheduled_increment.sub();
    wait_timer.reset();
    exception = ptr;

    // Notify all preempted threads to wake and throw an exception
    for (auto & cv : threads.wake)
        cv.notify_one();

    LOG_EVENT(F);
}

void CPULeaseAllocation::grant()
{
    // This code runs in the scheduler thread, so we have to keep it fast and simple
    std::unique_lock lock{mutex};
    requests.scheduled();
    scheduled_increment.sub();
    wait_timer.reset();
    grantImpl(lock);
}

void CPULeaseAllocation::grantImpl(std::unique_lock<std::mutex> & lock)
{
    // Cycle is required to deal with noncompeting requests, so the main case is a single iteration here
    do
    {
        ++allocated;
        ++granted;
        if (granted > 0 && !shutdown)
            acquirable.store(true, std::memory_order_relaxed);
        LOG_EVENT(G);
        requests.granted();
    } while (!schedule(lock));

    // Resume preempted threads if necessary
    while (granted > 0)
    {
        // We are trying to wake inactive thread with lowest thread number to increase utilization of lower threads
        if (size_t thread_num = threads.preempted.find_first(); thread_num != boost::dynamic_bitset<>::npos)
            resetPreempted(thread_num);
        else
            break; // No preempted threads, we are done
    }

    // TODO(serxa): we should release granted but not acquired slots after some timeout, to avoid unnecessary overprovisioning, but this requires modification of the PipelineExecutor as well
}

bool CPULeaseAllocation::renew(Lease & lease)
{
    UInt64 thread_time_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    chassert(thread_time_ns >= lease.last_report_ns); // This is guaranteed on Linux for thread clock
    ResourceCost delta_ns = thread_time_ns - lease.last_report_ns;
    if (delta_ns < settings.report_ns)
        return true; // Not enough time passed to report
    lease.last_report_ns = thread_time_ns;

    std::optional<OpenTelemetry::SpanHolder> report_span;
    if (settings.trace_cpu_scheduling)
    {
        report_span.emplace("CPU_LEASE_REPORT");
        report_span->addAttribute("workload", settings.workload);
        report_span->addAttribute("lease_id", lease_id);
        report_span->addAttribute("thread_number", lease.slot_id);
        report_span->addAttribute("delta_ns", delta_ns);
    }

    std::unique_lock lock{mutex};

    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));

    // Real-time acceleration for inelastic queries: the master thread (slot 0) runs the bottleneck
    // alone under SCHED_FIFO while workers downscale. renew() runs on the calling thread's own OS
    // thread, so SCHED_FIFO can be toggled on `self`.
    updateElasticity();
    const bool is_inelastic = isInelasticLocked();

    if (lease.is_realtime.load(std::memory_order_relaxed))
    {
        // This is the master thread while it is real-time. It runs "for free": its CPU time (delta_ns)
        // is discarded (not accounted, no MLFQ banding) and it is never preempted.
        if (shutdown)
        {
            disableRealtimeOnSelf(lease); // permit already returned by free()
            downscale(lease.slot_id, /* shutdown = */ true);
            lease.reset();
            return false;
        }
        if (is_inelastic)
        {
            report_span.reset();
            return true;
        }
        exitRealtimeMode(lock, lease); // Query became elastic: revert and fall through to normal accounting.
    }
    else if (realtime_mode && lease.slot_id != 0)
    {
        // The master is the real-time thread; this is a worker. Downscale so the master runs alone.
        // The executor re-spawns workers once the query leaves real-time mode (see exitRealtimeMode).
        // `acquirable` is forced false so the freed slot is not immediately re-acquired during RT.
        downscale(lease.slot_id);
        acquirable.store(false, std::memory_order_relaxed);
        lease.reset();
        return false;
    }
    else if (!realtime_disabled && !shutdown)
    {
        // Not in real-time mode yet. Track the inelastic-phase start (wall-clock); only the master
        // thread starts real-time mode, and only after the query stays inelastic long enough (which
        // filters out transient inelastic blips of otherwise elastic queries).
        if (is_inelastic)
        {
            const ResourceCost now_mono_ns = static_cast<ResourceCost>(clock_gettime_ns(CLOCK_MONOTONIC));
            if (inelastic_since_ns < 0)
                inelastic_since_ns = now_mono_ns;

            if (lease.slot_id == 0 && now_mono_ns - inelastic_since_ns >= kRealtimeInelasticThresholdNs)
            {
                enterRealtimeMode(lock, lease);
                if (lease.is_realtime.load(std::memory_order_relaxed))
                {
                    report_span.reset();
                    return true;
                }
            }
        }
        else
            inelastic_since_ns = -1;
    }

    consume(lock, delta_ns);

    report_span.reset();

    if (shutdown) // Allocation is being destroyed, worker thread should stop
    {
        downscale(lease.slot_id, /* shutdown = */ true);
        lease.reset();
        return false;
    }

    // Check if we need to decrease number of running threads (i.e. `acquired`).
    // We want number of `acquired` slots to be less than number of `allocated` slots.
    // Difference `allocated - acquired` equals `granted`. But we allow `granted == -1` for two reasons:
    //  1. To avoid preemption of master thread just after start.
    //     `acquire()` provides acquired slot "in credit" before it's granted to avoid delay.
    //  2. To avoid preemption of the last thread and allow 100% utilization with one "background" resource request.
    //     Otherwise every lease renewal leads to preemption of the last thread.
    // When requested, but not granted resource is consumed we have to do preemption (even for master thread).
    if (granted + static_cast<Int64>(requests.hasEnqueued()) < 0 || consumed_ns >= requested_ns)
    {
        // Check if preemption is needed
        size_t thread_num = lease.slot_id;
        if (thread_num == threads.last_running)
        {
            // Preemption. If we run more thread than we have slots, the last thread should wait for the next slot to be granted.
            // We only preempt the last running thread to avoid running many threads with low utilization (e.g spread 2 CPU among 10 threads).
            // It is better to run less threads, but utilize CPU better to avoid frequent context switches. This is how down-scaling works.
            setPreempted(thread_num);

            std::optional<OpenTelemetry::SpanHolder> preemption_span;
            if (settings.trace_cpu_scheduling)
            {
                preemption_span.emplace("CPU_LEASE_PREEMPTION");
                preemption_span->addAttribute("workload", settings.workload);
                preemption_span->addAttribute("lease_id", lease_id);
                preemption_span->addAttribute("thread_number", thread_num);
                preemption_span->addAttribute("consumed_ns", consumed_ns);
                preemption_span->addAttribute("requested_ns", requested_ns);
                preemption_span->addAttribute("enqueued", requests.hasEnqueued());
                preemption_span->addAttribute("allocated", allocated);
                preemption_span->addAttribute("running", threads.running_count);
            }

            auto preemption_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrencyControlPreemptedMicroseconds);
            CurrentMetrics::Increment preempted_increment(CurrentMetrics::ConcurrencyControlPreempted);
            acquired_increment.sub(1);

            bool wait_succeeded = waitForGrant(lock, thread_num);
            if (!wait_succeeded || shutdown)
            {
                // Timeout or exception or shutdown - worker thread should stop
                // Only count as downscale if actually timed out, not just shutdown
                downscale(thread_num, /* shutdown = */ wait_succeeded);
                lease.reset();
                return false;
            }

            if (settings.on_resume)
                settings.on_resume(thread_num);

            if (exception) // Stop the query
                throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));

            acquired_increment.add(1);
            // There is no need in updating lease.last_report_ns because it counts only CPU time, not waiting time
        }
    }
    return true;
}

bool CPULeaseAllocation::waitForGrant(std::unique_lock<std::mutex> & lock, size_t thread_num)
{
    auto timeout = thread_num == 0
        ? std::chrono::milliseconds::max() // Never involuntary stop the master thread - only downscale worker threads
        : settings.preemption_timeout;

    auto predicate = [this, thread_num]
    {
        return !threads.preempted[thread_num] || exception || shutdown;
    };

    // It is important to call on_preempt w/o lock to avoid deadlock due to recursive locking:
    // renew() -> ExecutorTasks::preempt() -> ExecutorTasks::finish() -> free()
    if (settings.on_preempt)
    {
        lock.unlock();
        try
        {
            settings.on_preempt(thread_num);
        }
        catch (...)
        {
            lock.lock();
            throw;
        }
        lock.lock();
    }

    if (timeout == std::chrono::milliseconds::max())
    {
        threads.wake[thread_num].wait(lock, predicate);
        return true; // Granted
    }
    else
    {
        return threads.wake[thread_num].wait_for(lock, timeout, predicate);
    }
}

void CPULeaseAllocation::consume(std::unique_lock<std::mutex> & lock, ResourceCost delta_ns)
{
    consumed_ns += delta_ns;
    if (allocated > 0 && consumed_ns >= requests.getMaxConsumed())
    {
        --allocated;
        --granted;
        if (granted <= 0 && !exception)
            acquirable.store(false, std::memory_order_relaxed);
        requests.finish();
        LOG_EVENT(C);
        if (!requests.hasEnqueued()) // In case if we renew the last slot, otherwise the next request is already scheduled
        {
            if (!schedule(lock))
                grantImpl(lock);
        }
        else
        {
            // A request is already enqueued, but `allocated` just dropped. That may move the query
            // into a lower (higher-priority) parallelism layer, so re-bucket the pending request in
            // place to keep parallelism leveling correct (promotion across a layer boundary).
            requests.reprioritize(computeRequestPriority());
        }
        // NOTE: we do not finish more than one request per one report to avoid stalling the pipeline for reports larger than quantum
    }
}

size_t CPULeaseAllocation::computeCap() const
{
    /// Upper bound on in-flight CPU slot requests.
    /// The hard cap is `max_threads`. If the pipeline exposes its number of ready tasks,
    /// we additionally clamp to `max(1, running_count + tasks / 3)` to avoid over-provisioning
    /// CPU quanta for queries that cannot keep `max_threads` threads busy (e.g. blocked by a
    /// pipeline breaker). Rationale for each term:
    ///  - `running_count` keeps enough in-flight quanta to cover every currently running thread
    ///    so consumption does not starve them;
    ///  - `tasks / 3` adds headroom proportional to the amount of parallelizable work available;
    ///  - the `max(..., 1)` floor guarantees progress at construction time (pipeline queues are
    ///    empty and no thread is running yet, so without the floor the first request would be
    ///    refused) and keeps at least one request in flight so `consume()` can re-evaluate the
    ///    cap as new tasks appear.
    size_t cap = max_threads;
    if (settings.get_tasks_count)
    {
        size_t tasks_count = settings.get_tasks_count() + threads.running_count;
        cap = std::max<size_t>(std::min<size_t>(max_threads, tasks_count), 2);
    }
    return cap;
}

void CPULeaseAllocation::updateElasticity()
{
    /// Demand below this threshold makes a query switch to "inelastic" mode.
    static constexpr size_t kInelasticEnterThreshold = 4; /// tasks + running < 4  -> inelastic
    /// Demand above this threshold makes a query switch back to "elastic" mode.
    /// The gap (4..8) is a buffer zone that keeps the current mode to avoid flapping.
    static constexpr size_t kElasticEnterThreshold = 8; /// tasks + running > 8  -> elastic
    static_assert(kInelasticEnterThreshold <= kElasticEnterThreshold, "Inelastic enter threshold must not exceed elastic enter threshold");

    if (!settings.get_tasks_count)
        return; // No task information available; preserve elastic behavior (cap == max_threads path)

    size_t demand = settings.get_tasks_count() + threads.running_count;
    if (inelastic)
    {
        if (demand > kElasticEnterThreshold)
            inelastic = false;
    }
    else
    {
        if (demand < kInelasticEnterThreshold)
            inelastic = true;
    }
}

Priority CPULeaseAllocation::computeRequestPriority() const
{
    /// Number of allocated slots that fit in one parallelism layer. A query gets `kLevelingThreads`
    /// slots at top priority (layer 0), the next `kLevelingThreads` at the next layer, and so on.
    static constexpr size_t kLevelingThreads = 8;
    static_assert(kLevelingThreads > 0, "kLevelingThreads must be positive to avoid division by zero");

    /// Parallelism leveling: a query with fewer allocated slots sits in a lower (higher-priority)
    /// layer and therefore strictly outranks a query that already runs more threads. Very wide
    /// queries are clamped to the last layer.
    size_t layer = std::min<size_t>(allocated / kLevelingThreads, MultiLevelFeedbackQueue::kNumLayers - 1);

    /// Within a layer, the sub-band is picked from `requested_ns` (cumulative consumed + outstanding
    /// granted quantum budget), so a query that has used less CPU sits in a higher (lower-index) band.
    /// The same thresholds are reused identically in every layer.
    Priority::Value band = MultiLevelFeedbackQueue::pickCpuBand(requested_ns);

    Priority priority{};
    priority.value = static_cast<Priority::Value>(layer * MultiLevelFeedbackQueue::kLayerWidth) + band;
    return priority;
}

void CPULeaseAllocation::enterRealtimeMode(std::unique_lock<std::mutex> & lock, Lease & lease)
{
    // IMPORTANT: must run on the master thread (it switches the caller's scheduling policy).
    chassert(!realtime_mode);
    chassert(lease.slot_id == 0);

    // Bound how many queries may monopolize a core at once (non-blocking).
    if (!RealTimeSlotPool::instance().tryAcquire())
        return; // No permit available right now; stay normal and retry on a later renew.

    if (!OSThreadRealtime::enable(settings.realtime_priority))
    {
        // Could not switch to SCHED_FIFO (e.g. no CAP_SYS_NICE): stop retrying for this allocation to
        // avoid hammering the syscall on every renew, and return the unusable permit to the pool.
        RealTimeSlotPool::instance().release();
        realtime_disabled = true;
        return;
    }

    realtime_mode = true;
    realtime_permit_held = true;
    lease.is_realtime.store(true, std::memory_order_relaxed);

    // Stop the scheduler from granting more slots: cancel the pending request and prevent the
    // executor from acquiring slots while the master runs alone. Workers downscale on their own renew.
    requests.cancel(lock);
    acquirable.store(false, std::memory_order_relaxed);
}

void CPULeaseAllocation::disableRealtimeOnSelf(Lease & lease)
{
    // IMPORTANT: must run on the OS thread that owns `lease`. Only reverts the OS scheduling policy;
    // permit / mode bookkeeping is handled by the caller.
    if (!lease.is_realtime.load(std::memory_order_relaxed))
        return;
    OSThreadRealtime::disable();
    lease.is_realtime.store(false, std::memory_order_relaxed);
}

void CPULeaseAllocation::exitRealtimeMode(std::unique_lock<std::mutex> & lock, Lease & lease)
{
    // IMPORTANT: must run on the master thread.
    disableRealtimeOnSelf(lease);
    if (realtime_permit_held)
    {
        RealTimeSlotPool::instance().release();
        realtime_permit_held = false;
    }
    realtime_mode = false;
    inelastic_since_ns = -1; // Require a fresh inelastic phase before re-entering real-time mode.

    // Resume normal scheduling: allow the executor to re-acquire the slots freed by downscaled workers
    // and request more if needed, so it can re-spawn worker threads as parallel work appears.
    acquirable.store(granted > 0 && !shutdown, std::memory_order_relaxed);
    if (!shutdown)
        schedule(lock);
}

void CPULeaseAllocation::relinquishRealtime(Lease & lease)
{
    std::unique_lock lock{mutex};
    if (lease.is_realtime.load(std::memory_order_relaxed))
        exitRealtimeMode(lock, lease);
}

bool CPULeaseAllocation::schedule(std::unique_lock<std::mutex> &)
{
    size_t cap = computeCap();
    if (allocated == max_threads || shutdown)
        return true;

    Priority priority = computeRequestPriority();

    ResourceCost cost = settings.quantum_ns + std::max<ResourceCost>(0, consumed_ns - requested_ns);
    requested_ns += cost;
    const auto enqueue_result = requests.enqueue(cost, requested_ns, priority, cap < allocated);
    if (enqueue_result == RequestChain::EnqueueResult::Enqueued)
    {
        scheduled_increment.add();
        wait_timer.emplace(wait_counters->timer(ProfileEvents::ConcurrencyControlWaitMicroseconds));
        LOG_EVENT(E);
        return true;
    }
    if (enqueue_result == RequestChain::EnqueueResult::Throttled)
        return true;
    return false; // Request is noncompeting and should be granted immediately
}

void CPULeaseAllocation::release(Lease & lease)
{
    UInt64 thread_time_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    chassert(thread_time_ns >= lease.last_report_ns); // This is guaranteed on Linux for thread clock
    ResourceCost delta_ns = thread_time_ns - lease.last_report_ns;
    lease.last_report_ns = thread_time_ns;

    // Report the last chunk of consumed resource
    std::unique_lock lock{mutex};

    if (lease.is_realtime.load(std::memory_order_relaxed))
    {
        // The master is releasing its lease while still real-time. Revert the OS policy and return the
        // permit, and discard the trailing delta since it ran "for free". No re-scheduling here: the
        // thread is stopping (the allocation is freed shortly after).
        disableRealtimeOnSelf(lease);
        if (realtime_permit_held)
        {
            RealTimeSlotPool::instance().release();
            realtime_permit_held = false;
        }
        realtime_mode = false;
        delta_ns = 0;
    }

    try
    {
        consume(lock, delta_ns);
    }
    catch (const Exception & e)
    {
        // `consume` may call `schedule` which may call `enqueueRequest` on a scheduler queue
        // that is being destructed (e.g. when a workload is dropped while queries are still running).
        // Since `release` is called from Lease destructor, we must not throw.
        if (e.code() != ErrorCodes::INVALID_SCHEDULER_NODE)
            throw;
    }

    // Release the slot
    downscale(lease.slot_id);
}

bool CPULeaseAllocation::isRequesting() const
{
    std::lock_guard lock{mutex};
    return requests.hasEnqueued();
}

}
