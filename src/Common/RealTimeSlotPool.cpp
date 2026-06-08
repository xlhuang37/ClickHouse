#include <Common/RealTimeSlotPool.h>


namespace DB
{

RealTimeSlotPool & RealTimeSlotPool::instance()
{
    static RealTimeSlotPool pool;
    return pool;
}

void RealTimeSlotPool::setMaxSlots(size_t value)
{
    max_slots.store(value, std::memory_order_relaxed);
}

bool RealTimeSlotPool::tryAcquire()
{
    size_t current = used_slots.load(std::memory_order_relaxed);
    while (true)
    {
        /// Re-read the limit on every iteration so a concurrent `setMaxSlots` is respected.
        if (current >= max_slots.load(std::memory_order_relaxed))
            return false;
        if (used_slots.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
            return true;
        /// `current` was updated with the latest value by compare_exchange_weak; retry.
    }
}

void RealTimeSlotPool::release()
{
    used_slots.fetch_sub(1, std::memory_order_acq_rel);
}

}
