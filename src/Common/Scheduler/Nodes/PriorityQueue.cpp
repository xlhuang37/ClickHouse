#include <Common/Scheduler/Nodes/PriorityQueue.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB
{

void registerPriorityQueue(SchedulerNodeFactory & factory)
{
    /// NOTE: `"priority"` is already used by `PriorityPolicy` (internal non-leaf node),
    /// so we register this leaf queue under `"priority_queue"`.
    factory.registerMethod<PriorityQueue>("priority_queue");
}

}
