#include <Common/Scheduler/Nodes/MultiLevelFeedbackQueue.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Exception.h>

#include <algorithm>
#include <exception>
#include <mutex>
#include <vector>


namespace DB
{

namespace
{
    /// Process-wide MLFQ topology. Captured once at server startup (see `initTopologyOnce`) and
    /// immutable afterwards, which makes it a restart-only setting. Defaults match the historical
    /// hardcoded constants so tests and non-server callers behave as before.
    MultiLevelFeedbackQueue::Topology & mutableTopology()
    {
        static MultiLevelFeedbackQueue::Topology instance;
        return instance;
    }

    std::once_flag topology_init_flag;
}

const MultiLevelFeedbackQueue::Topology & MultiLevelFeedbackQueue::topology()
{
    return mutableTopology();
}

void MultiLevelFeedbackQueue::initTopologyOnce(size_t layer_width, size_t num_layers, size_t leveling_threads)
{
    std::call_once(topology_init_flag, [&]
    {
        Topology & t = mutableTopology();
        t.layer_width = std::max<size_t>(layer_width, 1);
        t.num_layers = std::max<size_t>(num_layers, 1);
        t.leveling_threads = std::max<size_t>(leveling_threads, 1);

        /// Bucket indices are stored as `std::uint8_t`, so the total number of priority levels
        /// must stay within [1, 255]. Reduce the number of layers (then the layer width) to fit.
        static constexpr size_t kMaxPriorityLevels = 255;
        t.layer_width = std::min<size_t>(t.layer_width, kMaxPriorityLevels);
        if (t.layer_width * t.num_layers > kMaxPriorityLevels)
            t.num_layers = std::max<size_t>(kMaxPriorityLevels / t.layer_width, 1);
    });
}

Priority::Value MultiLevelFeedbackQueue::pickCpuBand(ResourceCost cumulative_cpu_ns, const std::vector<ResourceCost> & finite_thresholds)
{
    /// Clamp negatives (e.g. arithmetic overflow upstream) to zero so we still hit band 0.
    if (cumulative_cpu_ns < 0)
        cumulative_cpu_ns = 0;

    /// The lowest band index is the implicit catch-all for everything above the finite thresholds.
    const size_t last_band = topology().layer_width - 1;

    /// Linear scan over a tiny array. The first threshold the value falls under selects the band;
    /// a value above every finite threshold lands in the catch-all (last) band.
    for (size_t i = 0; i < finite_thresholds.size(); ++i)
    {
        if (cumulative_cpu_ns < finite_thresholds[i])
            return static_cast<Priority::Value>(std::min(i, last_band));
    }
    return static_cast<Priority::Value>(last_band);
}

void MultiLevelFeedbackQueue::enqueueRequest(ResourceRequest * request, Priority priority)
{
    std::lock_guard lock(mutex);
    if (is_not_usable)
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue is about to be destructed");

    if (total_size >= static_cast<size_t>(info.queue_size))
        throw Exception(ErrorCodes::SERVER_OVERLOADED, "Workload limit `max_waiting_queries` has been reached: {} of {}", total_size, info.queue_size);

    /// Clamp `priority.value` into [0, buckets.size()-1]. Callers may legitimately pass
    /// `default_priority` (value 0) or, historically, very negative / very large values:
    /// clamping keeps the queue robust to such inputs while preserving the intended
    /// "smaller value = higher priority" semantics.
    Priority::Value v = priority.value;
    if (v < 0)
        v = 0;
    if (v > static_cast<Priority::Value>(buckets.size() - 1))
        v = static_cast<Priority::Value>(buckets.size() - 1);
    auto idx = static_cast<std::uint8_t>(v);

    queue_cost += request->cost;
    bool was_empty = (total_size == 0);
    buckets[idx].push_back(*request);
    bucket_of.emplace(request, idx);
    ++total_size;
    if (was_empty)
        scheduleActivation();
}

std::pair<ResourceRequest *, bool> MultiLevelFeedbackQueue::dequeueRequest()
{
    std::lock_guard lock(mutex);
    if (total_size == 0)
        return {nullptr, false};

    /// Linear scan over the buckets; lowest index == highest priority.
    for (size_t idx = 0; idx < buckets.size(); ++idx)
    {
        auto & bucket = buckets[idx];
        if (bucket.empty())
            continue;

        ResourceRequest * result = &bucket.front();
        bucket.pop_front();
        bucket_of.erase(result);
        --total_size;

        if (total_size == 0)
        {
            busy_periods++;
            event_queue->cancelActivation(this); /// Avoid scheduling two activations which leads to crash.
        }
        queue_cost -= result->cost;
        incrementDequeued(result->cost);
        return {result, total_size > 0};
    }

    /// Unreachable: total_size > 0 implies at least one non-empty bucket.
    chassert(false);
    return {nullptr, false};
}

bool MultiLevelFeedbackQueue::cancelRequest(ResourceRequest * request)
{
    std::lock_guard lock(mutex);
    if (is_not_usable)
        return false; /// Any request should already be failed or executed.
    if (!request->is_linked())
        return false;

    auto lookup = bucket_of.find(request);
    if (lookup == bucket_of.end())
    {
        /// Not in this queue. Same caveat as `FifoQueue::cancelRequest` applies: it
        /// is up to the caller to make sure `request` was enqueued here.
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "trying to cancel request (linked into another queue) unknown to this queue: {}",
            getPath());
    }

    std::uint8_t idx = lookup->second;
    auto & bucket = buckets[idx];
    if (bucket.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "trying to cancel request (linked into another queue) from empty priority bucket in queue: {}",
            getPath());

    bucket.erase(bucket.iterator_to(*request));
    bucket_of.erase(lookup);
    --total_size;

    if (total_size == 0)
    {
        busy_periods++;
        event_queue->cancelActivation(this);
    }
    queue_cost -= request->cost;
    canceled_requests++;
    canceled_cost += request->cost;
    return true;
}

bool MultiLevelFeedbackQueue::reprioritizeRequest(ResourceRequest * request, Priority priority)
{
    std::lock_guard lock(mutex);
    if (is_not_usable)
        return false; /// Any request should already be failed or executed.

    auto lookup = bucket_of.find(request);
    if (lookup == bucket_of.end())
        return false; /// Not enqueued here (e.g. already dequeued by the scheduler thread).

    /// Clamp the requested level into [0, buckets.size()-1], mirroring enqueueRequest().
    Priority::Value v = priority.value;
    if (v < 0)
        v = 0;
    if (v > static_cast<Priority::Value>(buckets.size() - 1))
        v = static_cast<Priority::Value>(buckets.size() - 1);
    auto new_idx = static_cast<std::uint8_t>(v);

    std::uint8_t old_idx = lookup->second;
    if (new_idx == old_idx)
        return true; /// Already in the right bucket, nothing to do.

    /// Move the request between buckets in place. `total_size`/`queue_cost` are unchanged and
    /// the queue is already active (it holds this request), so no (de)activation is required.
    auto & old_bucket = buckets[old_idx];
    old_bucket.erase(old_bucket.iterator_to(*request));
    buckets[new_idx].push_back(*request);
    lookup->second = new_idx;
    return true;
}

void MultiLevelFeedbackQueue::purgeQueue()
{
    /// Collect requests to fail while holding the lock, but call failed() outside the lock
    /// to avoid potential deadlock with CPULeaseAllocation::mutex (lock order inversion).
    std::vector<ResourceRequest *> requests_to_fail;
    {
        std::lock_guard lock(mutex);
        is_not_usable = true;
        for (auto & bucket : buckets)
        {
            while (!bucket.empty())
            {
                ResourceRequest * request = &bucket.front();
                bucket.pop_front();
                requests_to_fail.push_back(request);
            }
        }
        bucket_of.clear();
        total_size = 0;
        event_queue->cancelActivation(this);
    }
    auto exception = std::make_exception_ptr(
        Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue with resource request is about to be destructed"));
    for (ResourceRequest * request : requests_to_fail)
        request->failed(exception);
}

void registerMultiLevelFeedbackQueue(SchedulerNodeFactory & factory)
{
    /// Register under the legacy name `"priority_queue"` so that
    /// `UnifiedSchedulerNode` and any existing workload configs keep working.
    /// Also expose `"mlfq"` as a clearer alias.
    factory.registerMethod<MultiLevelFeedbackQueue>("priority_queue");
    factory.registerMethod<MultiLevelFeedbackQueue>("mlfq");
}

}
