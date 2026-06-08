#pragma once

#include <base/types.h>


namespace DB
{

/// Switch the current OS thread between the default scheduling policy and Linux real-time
/// scheduling (`SCHED_FIFO`). Both helpers operate on the calling thread only.
///
/// Real-time scheduling lets a thread monopolize a CPU core, so it is gated behind the
/// `CAP_SYS_NICE` capability. On non-Linux platforms, or without the capability, `enable` is a
/// no-op that returns false and `disable` is a no-op.
struct OSThreadRealtime
{
    /// Switch the current thread to `SCHED_FIFO` with the given priority.
    /// Returns true iff the policy was actually changed. Never throws.
    static bool enable(Int32 priority);

    /// Switch the current thread back to the default policy (`SCHED_OTHER`). Never throws.
    static void disable();
};

}
