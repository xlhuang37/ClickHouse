#pragma once

#include <Common/Priority.h>
#include <Common/Scheduler/ISchedulerQueue.h>


namespace DB
{

/*
 * Priority-aware leaf queue. Requests enqueued with a smaller `Priority::Value`
 * are dequeued first; within a single priority level FIFO order is preserved.
 *
 * Callers that do not care about priorities keep using the inherited
 * `enqueueRequest(ResourceRequest *)` overload, which delegates to the
 * priority-aware variant with `default_priority`.
 */
class ISchedulerPriorityQueue : public ISchedulerQueue
{
public:
    using ISchedulerQueue::ISchedulerQueue;

    /// Priority used for the legacy priority-less `enqueueRequest(request)` overload.
    static constexpr Priority default_priority{};

    /// Enqueue new request with the given priority. Lower value = higher priority
    /// (matches `Common/Priority.h` and `PriorityPolicy`).
    /// Must be thread-safe.
    virtual void enqueueRequest(ResourceRequest * request, Priority priority) = 0;

    /// Final override of the legacy priority-less entry point. Existing callers keep
    /// working unchanged and their requests end up at `default_priority`.
    void enqueueRequest(ResourceRequest * request) final
    {
        enqueueRequest(request, default_priority);
    }
};

}
