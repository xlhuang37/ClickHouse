#pragma once

#include <Common/Priority.h>
#include <Common/Scheduler/ISchedulerPriorityQueue.h>
#include <Common/Scheduler/ResourceRequest.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/intrusive/list.hpp>

#include <array>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SCHEDULER_NODE;
    extern const int SERVER_OVERLOADED;
}

/*
 * Multi-Level Feedback Queue (MLFQ) leaf scheduler queue.
 *
 * Unlike `PriorityQueue` (which kept an unbounded `std::map<Priority::Value, list>` and
 * therefore created a fresh bucket per distinct priority value), MLFQ keeps a fixed,
 * small set of priority levels in a contiguous `std::array`. This:
 *  - bounds the number of buckets to a compile-time constant,
 *  - avoids `std::map` node allocations on every new priority value,
 *  - removes the "infinitesimal granularity" thrash where two requests with priority
 *    values one apart would constantly preempt each other.
 *
 * Levels 0..(kPriorityLevels-1) are elastic bands; the caller decides which band by passing
 * a `Priority{value}` whose `value` is the bucket index. Any out-of-range value is clamped
 * into [0, kPriorityLevels-1].
 *
 * Mapping a query's cumulative CPU consumption (consumed + granted, i.e.
 * `CPULeaseAllocation::requested_ns`) to a CPU band within a layer is done by `pickCpuBand`
 * (see .cpp); the parallelism layer offset is added by `CPULeaseAllocation`.
 *
 * Dequeue scans levels 0..K-1 in order and pops the FIFO front of the first non-empty
 * bucket. Cancel is O(1) via a `request -> bucket_index` reverse map.
 *
 * The factory is registered under the same `"priority_queue"` name as the previous
 * `PriorityQueue` so that workload configs and `UnifiedSchedulerNode` continue to work
 * unchanged, plus a `"mlfq"` alias for clarity.
 */
class MultiLevelFeedbackQueue final : public ISchedulerPriorityQueue
{
public:
    /// Parallelism leveling topology. The elastic levels are partitioned into `num_layers`
    /// contiguous "layers", each `layer_width` levels wide. A query's layer is chosen by its
    /// current parallelism (allocated slots / `leveling_threads`), and the sub-level within a
    /// layer is chosen by cumulative CPU consumption. A query in a lower layer (less parallelism)
    /// always outranks a query in a higher layer, regardless of CPU age.
    ///
    /// The topology is a restart-only server setting: it is captured once at startup via
    /// `initTopologyOnce` and is immutable afterwards. Defaults match the historical constants.
    struct Topology
    {
        size_t layer_width = 4; /// Number of priority levels (CPU bands) per parallelism layer.
        size_t num_layers = 2; /// Number of parallelism layers.
        size_t leveling_threads = 8; /// Number of allocated slots that map to one parallelism layer.

        /// Total number of priority levels (bucket count). Level 0 = highest.
        size_t priorityLevels() const { return layer_width * num_layers; }
    };

    /// Returns the process-wide MLFQ topology (see `initTopologyOnce`).
    static const Topology & topology();

    /// Capture the topology from server settings. Only the first call takes effect; subsequent
    /// calls are ignored, which makes the topology a restart-only setting. Each value is
    /// sanitized to be at least 1.
    static void initTopologyOnce(size_t layer_width, size_t num_layers, size_t leveling_threads);

    MultiLevelFeedbackQueue(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
        : ISchedulerPriorityQueue(event_queue_, config, config_prefix)
        , buckets(topology().priorityLevels())
    {}

    MultiLevelFeedbackQueue(EventQueue * event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerPriorityQueue(event_queue_, info_)
        , buckets(topology().priorityLevels())
    {}

    ~MultiLevelFeedbackQueue() override
    {
        purgeQueue();
    }

    const String & getTypeName() const override
    {
        static String type_name("multi_level_feedback_queue");
        return type_name;
    }

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * _ = dynamic_cast<MultiLevelFeedbackQueue *>(other))
            return true;
        return false;
    }

    using ISchedulerPriorityQueue::enqueueRequest;

    void enqueueRequest(ResourceRequest * request, Priority priority) override;
    std::pair<ResourceRequest *, bool> dequeueRequest() override;
    bool cancelRequest(ResourceRequest * request) override;
    bool reprioritizeRequest(ResourceRequest * request, Priority priority) override;
    void purgeQueue() override;

    bool isActive() override
    {
        std::lock_guard lock(mutex);
        return total_size > 0;
    }

    size_t activeChildren() override
    {
        return 0;
    }

    void activateChild(ISchedulerNode *) override
    {
        assert(false); /// Queue cannot have children.
    }

    void attachChild(const SchedulerNodePtr &) override
    {
        throw Exception(
            ErrorCodes::INVALID_SCHEDULER_NODE,
            "Cannot add child to leaf scheduler queue: {}",
            getPath());
    }

    void removeChild(ISchedulerNode *) override
    {
    }

    ISchedulerNode * getChild(const String &) override
    {
        return nullptr;
    }

    std::pair<UInt64, Int64> getQueueLengthAndCost()
    {
        std::lock_guard lock(mutex);
        return {total_size, queue_cost};
    }

    /// Map a cumulative CPU consumption value (in nanoseconds; we use
    /// `CPULeaseAllocation::requested_ns` = consumed + granted) to a CPU band, i.e. a
    /// sub-level within a parallelism layer in the range [0, topology().layer_width - 1].
    /// `finite_thresholds` holds the per-band upper bounds (exclusive); the last band is an
    /// implicit catch-all. The caller adds the layer offset to obtain an absolute level.
    static Priority::Value pickCpuBand(ResourceCost cumulative_cpu_ns, const std::vector<ResourceCost> & finite_thresholds);

private:
    std::mutex mutex;
    Int64 queue_cost = 0;
    /// Buckets sized to `topology().priorityLevels()` at construction (never resized afterwards,
    /// so intrusive list heads stay stable). `buckets[0]` is highest priority.
    /// Each bucket keeps FIFO order within its priority level.
    std::vector<boost::intrusive::list<ResourceRequest>> buckets;
    /// Reverse lookup for O(1) cancel.
    std::unordered_map<ResourceRequest *, std::uint8_t> bucket_of;
    size_t total_size = 0;
    bool is_not_usable = false;
};

}
