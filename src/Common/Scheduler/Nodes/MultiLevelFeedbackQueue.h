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


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SCHEDULER_NODE;
    extern const int SERVER_OVERLOADED;
}

/*
 * Multi-Level Feedback Queue (MLFQ) leaf scheduler queue with "decay fairness".
 *
 * Unlike `PriorityQueue` (which kept an unbounded `std::map<Priority::Value, list>` and
 * therefore created a fresh bucket per distinct priority value), MLFQ keeps a fixed,
 * small set of priority levels in a contiguous `std::array`. This:
 *  - bounds the number of buckets to a compile-time constant,
 *  - avoids `std::map` node allocations on every new priority value,
 *  - removes the "infinitesimal granularity" thrash where two requests with priority
 *    values one apart would constantly preempt each other.
 *
 * Levels 0..(kPriorityLevels-1) are elastic bands; the caller decides which band
 * by passing a `Priority{value}` whose `value` is the bucket index. Any out-of-range
 * value is clamped into [0, kPriorityLevels-1].
 *
 * Mapping a query's cumulative CPU consumption (consumed + granted, i.e.
 * `CPULeaseAllocation::requested_ns`) to a band is done by `pickElasticLevel()` (see .cpp).
 *
 * Decay fairness (instead of absolute priority): each level carries a virtual CPU time
 * `vtime` and a fixed `weight` (weight of level 0 is 1, later levels decay by a hardcoded
 * ratio). Dequeue serves the non-empty level with the smallest `vtime`, then advances that
 * level's `vtime` by `cost / weight`. Because higher-priority levels carry larger weights,
 * their virtual time grows more slowly and they receive a proportionally larger share of
 * CPU, while lower levels still make progress (decay, not starvation). A level that has
 * been idle has its `vtime` lifted to the system maximum on re-activation so it cannot
 * hoard CPU after returning (mirrors `FairPolicy`). Cancel is O(1) via a
 * `request -> bucket_index` reverse map.
 *
 * The factory is registered under the same `"priority_queue"` name as the previous
 * `PriorityQueue` so that workload configs and `UnifiedSchedulerNode` continue to work
 * unchanged, plus a `"mlfq"` alias for clarity.
 */
class MultiLevelFeedbackQueue final : public ISchedulerPriorityQueue
{
public:
    /// Number of priority levels. Level 0 = highest; level K-1 = lowest.
    /// Kept in the header so `CPULeaseAllocation` can reason about valid level indices.
    static constexpr size_t kPriorityLevels = 9;

    MultiLevelFeedbackQueue(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
        : ISchedulerPriorityQueue(event_queue_, config, config_prefix)
    {}

    MultiLevelFeedbackQueue(EventQueue * event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerPriorityQueue(event_queue_, info_)
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
    /// `CPULeaseAllocation::requested_ns` = consumed + granted) to an elastic band
    /// in the range [0, kPriorityLevels - 1].
    static Priority::Value pickElasticLevel(ResourceCost cumulative_cpu_ns);

private:
    std::mutex mutex;
    Int64 queue_cost = 0;
    /// Fixed-size buckets. Each bucket keeps FIFO order within its priority level.
    std::array<boost::intrusive::list<ResourceRequest>, kPriorityLevels> buckets;
    /// Decay-fairness virtual CPU time per level. Dequeue serves the non-empty level
    /// with the smallest `level_vtime`, then advances it by `cost / weight`.
    std::array<double, kPriorityLevels> level_vtime{};
    /// Running maximum of `level_vtime`, used to lift a re-activated idle level so it
    /// cannot hoard CPU after returning (see `FairPolicy`).
    double max_vtime = 0;
    /// Wall-clock of the last full reset of `level_vtime`, to bound floating-point drift.
    UInt64 last_reset_ns = 0;
    /// Reverse lookup for O(1) cancel.
    std::unordered_map<ResourceRequest *, std::uint8_t> bucket_of;
    size_t total_size = 0;
    bool is_not_usable = false;
};

}
