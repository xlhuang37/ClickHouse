#include <Common/OSThreadRealtime.h>

#include <Common/logger_useful.h>

#if defined(OS_LINUX)
#include <sched.h>
#include <cerrno>
#include <cstring>
#include <linux/capability.h>
#include <Common/hasLinuxCapability.h>
#endif


namespace DB
{

bool OSThreadRealtime::enable([[maybe_unused]] const Int32 priority)
{
#if defined(OS_LINUX)
    if (!hasLinuxCapability(CAP_SYS_NICE))
        return false;

    sched_param param{};
    param.sched_priority = priority;
    if (sched_setscheduler(0, SCHED_FIFO, &param) != 0)
    {
        LOG_WARNING(getLogger("OSThreadRealtime"),
            "Failed to switch thread to SCHED_FIFO with priority {}: {}", priority, std::strerror(errno));
        return false;
    }
    return true;
#else
    return false;
#endif
}

void OSThreadRealtime::disable()
{
#if defined(OS_LINUX)
    sched_param param{};
    param.sched_priority = 0;
    if (sched_setscheduler(0, SCHED_OTHER, &param) != 0)
    {
        LOG_WARNING(getLogger("OSThreadRealtime"),
            "Failed to restore thread to SCHED_OTHER: {}", std::strerror(errno));
    }
#endif
}

}
