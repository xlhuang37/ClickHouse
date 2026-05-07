#pragma once

#include <Common/Priority.h>
#include <Common/Scheduler/ISchedulerPriorityQueue.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/intrusive/list.hpp>

#include <map>
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
 * Priority queue to hold pending resource requests.
 *
 * Internally a `std::map<Priority::Value, intrusive_list<ResourceRequest>>` is kept ordered
 * ascending by priority value. `dequeueRequest()` always pops the front of the first
 * (lowest value => highest priority) non-empty bucket, so within a single priority level
 * requests keep FIFO order. A reverse `request -> priority` lookup table makes
 * `cancelRequest()` do bucket lookup in O(log P) where P is the number of distinct
 * priorities currently present.
 *
 * Behaviour notes:
 *  - When instantiated via the node factory (`"priority_queue"`) or the no-priority
 *    `enqueueRequest(ResourceRequest *)` overload, every request lands in
 *    `default_priority`, yielding FIFO semantics identical to `FifoQueue`.
 *  - The factory name is `"priority_queue"` because `"priority"` is already taken by
 *    `PriorityPolicy` (an internal, non-leaf scheduler node that orders children in the
 *    scheduler tree).
 */
class PriorityQueue final : public ISchedulerPriorityQueue
{
public:
    PriorityQueue(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
        : ISchedulerPriorityQueue(event_queue_, config, config_prefix)
    {}

    PriorityQueue(EventQueue * event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerPriorityQueue(event_queue_, info_)
    {}

    ~PriorityQueue() override
    {
        purgeQueue();
    }

    const String & getTypeName() const override
    {
        static String type_name("priority_queue");
        return type_name;
    }

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * _ = dynamic_cast<PriorityQueue *>(other))
            return true;
        return false;
    }

    using ISchedulerPriorityQueue::enqueueRequest;

    void enqueueRequest(ResourceRequest * request, Priority priority) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue is about to be destructed");

        if (total_size >= static_cast<size_t>(info.queue_size))
            throw Exception(ErrorCodes::SERVER_OVERLOADED, "Workload limit `max_waiting_queries` has been reached: {} of {}", total_size, info.queue_size);

        queue_cost += request->cost;
        bool was_empty = (total_size == 0);
        buckets[priority.value].push_back(*request);
        priority_of.emplace(request, priority.value);
        ++total_size;
        if (was_empty)
            scheduleActivation();
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        std::lock_guard lock(mutex);
        if (total_size == 0)
            return {nullptr, false};

        auto it = buckets.begin(); /// Lowest priority value == highest priority.
        auto & bucket = it->second;
        ResourceRequest * result = &bucket.front();
        bucket.pop_front();
        priority_of.erase(result);
        if (bucket.empty())
            buckets.erase(it);
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

    bool cancelRequest(ResourceRequest * request) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            return false; /// Any request should already be failed or executed.
        if (!request->is_linked())
            return false;

        auto lookup = priority_of.find(request);
        if (lookup == priority_of.end())
        {
            /// Not in this queue. Same caveat as `FifoQueue::cancelRequest` applies: it
            /// is up to the caller to make sure `request` was enqueued here.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "trying to cancel request (linked into another queue) unknown to this queue: {}",
                getPath());
        }

        Priority::Value pv = lookup->second;
        auto bucket_it = buckets.find(pv);
        if (bucket_it == buckets.end() || bucket_it->second.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "trying to cancel request (linked into another queue) from empty priority bucket in queue: {}",
                getPath());

        bucket_it->second.erase(bucket_it->second.iterator_to(*request));
        priority_of.erase(lookup);
        if (bucket_it->second.empty())
            buckets.erase(bucket_it);
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

    void purgeQueue() override
    {
        /// Collect requests to fail while holding the lock, but call failed() outside the lock
        /// to avoid potential deadlock with CPULeaseAllocation::mutex (lock order inversion).
        std::vector<ResourceRequest *> requests_to_fail;
        {
            std::lock_guard lock(mutex);
            is_not_usable = true;
            for (auto & [_, bucket] : buckets)
            {
                while (!bucket.empty())
                {
                    ResourceRequest * request = &bucket.front();
                    bucket.pop_front();
                    requests_to_fail.push_back(request);
                }
            }
            buckets.clear();
            priority_of.clear();
            total_size = 0;
            event_queue->cancelActivation(this);
        }
        auto exception = std::make_exception_ptr(
            Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue with resource request is about to be destructed"));
        for (ResourceRequest * request : requests_to_fail)
            request->failed(exception);
    }

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

private:
    std::mutex mutex;
    Int64 queue_cost = 0;
    /// Buckets ordered ascending by priority value => `begin()` is highest priority.
    /// Each bucket keeps FIFO order within its priority level.
    std::map<Priority::Value, boost::intrusive::list<ResourceRequest>> buckets;
    /// Reverse lookup for O(log P) cancel.
    std::unordered_map<ResourceRequest *, Priority::Value> priority_of;
    size_t total_size = 0;
    bool is_not_usable = false;
};

}
