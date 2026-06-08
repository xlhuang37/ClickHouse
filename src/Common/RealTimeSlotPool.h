#pragma once

#include <base/types.h>

#include <atomic>
#include <cstddef>


namespace DB
{

/** Process-wide pool of "real-time thread" slots.
  *
  * A slot represents the right for one query thread to run under Linux real-time scheduling
  * (`SCHED_FIFO`) and thus to monopolize a CPU core. The number of slots bounds how many
  * threads may be real-time at once and should not exceed the number of CPU cores that can be
  * safely dedicated to real-time work.
  *
  * This is a lock-free counting semaphore. Acquisition is strictly non-blocking: `tryAcquire`
  * returns `false` when the pool is exhausted or disabled, in which case the caller simply keeps
  * running under the default scheduling policy.
  *
  * The default number of slots is `kDefaultMaxSlots` so the feature is enabled out of the box,
  * even before the server applies configuration (and in non-server contexts such as unit tests).
  * Setting the count to 0 fully disables the feature.
  */
class RealTimeSlotPool
{
public:
    static constexpr size_t kDefaultMaxSlots = 8;

    static RealTimeSlotPool & instance();

    /// Set the maximum number of concurrent real-time slots. 0 disables the pool.
    void setMaxSlots(size_t value);

    size_t getMaxSlots() const { return max_slots.load(std::memory_order_relaxed); }
    size_t getUsedSlots() const { return used_slots.load(std::memory_order_relaxed); }

    /// Try to take one slot without blocking. Returns true iff a slot was acquired
    /// (the caller then owns it and must call `release` exactly once).
    [[nodiscard]] bool tryAcquire();

    /// Return a previously acquired slot.
    void release();

private:
    RealTimeSlotPool() = default;

    std::atomic<size_t> max_slots{kDefaultMaxSlots};
    std::atomic<size_t> used_slots{0};
};

}
