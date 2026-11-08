#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ISchedulerConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/RequestQueue.h>

#include <Common/Exception.h>

#include <ranges>

namespace DB
{

void ResourceRequest::finish()
{
    // Iterate over constraints in reverse order
    for (ISchedulerConstraint * constraint : std::ranges::reverse_view(constraints))
    {
        if (constraint)
            constraint->finishRequest(this);
    }

    // A granted `fcfs` or `if-fcfs` quantum holds the leaf against later arrivals until it is finished.
    if (ISchedulerNode * leaf = std::exchange(scheduling.fcfs_leaf, nullptr))
        static_cast<RequestQueue *>(leaf)->releaseFcfsInflight(*this);
}

bool ResourceRequest::addConstraint(ISchedulerConstraint * new_constraint)
{
    for (auto & constraint : constraints)
    {
        if (!constraint)
        {
            constraint = new_constraint;
            return true;
        }
    }
    return false;
}

}
