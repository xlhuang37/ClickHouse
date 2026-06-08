#include <gtest/gtest.h>

#include <Common/OSThreadRealtime.h>
#include <Common/RealTimeSlotPool.h>

#include <vector>

#if defined(OS_LINUX)
#include <sched.h>
#include <linux/capability.h>
#include <Common/hasLinuxCapability.h>
#endif

using namespace DB;

namespace
{

/// RAII helper that restores the pool's max-slot count after a test, so tests stay independent
/// despite the pool being a process-wide singleton.
struct MaxSlotsGuard
{
    size_t original;
    explicit MaxSlotsGuard(size_t value)
        : original(RealTimeSlotPool::instance().getMaxSlots())
    {
        RealTimeSlotPool::instance().setMaxSlots(value);
    }
    ~MaxSlotsGuard() { RealTimeSlotPool::instance().setMaxSlots(original); }
};

}

TEST(RealTimeSlotPool, DefaultIsEnabled)
{
    /// The feature must be on by default so it works without explicit server configuration.
    EXPECT_EQ(RealTimeSlotPool::kDefaultMaxSlots, 8u);
}

TEST(RealTimeSlotPool, AcquireRespectsBoundAndReleaseReturnsSlots)
{
    MaxSlotsGuard guard(3);
    auto & pool = RealTimeSlotPool::instance();

    ASSERT_EQ(pool.getUsedSlots(), 0u);

    EXPECT_TRUE(pool.tryAcquire());
    EXPECT_TRUE(pool.tryAcquire());
    EXPECT_TRUE(pool.tryAcquire());
    EXPECT_EQ(pool.getUsedSlots(), 3u);

    /// Pool exhausted.
    EXPECT_FALSE(pool.tryAcquire());
    EXPECT_EQ(pool.getUsedSlots(), 3u);

    /// Releasing a slot lets a new acquisition succeed.
    pool.release();
    EXPECT_EQ(pool.getUsedSlots(), 2u);
    EXPECT_TRUE(pool.tryAcquire());
    EXPECT_EQ(pool.getUsedSlots(), 3u);

    pool.release();
    pool.release();
    pool.release();
    EXPECT_EQ(pool.getUsedSlots(), 0u);
}

TEST(RealTimeSlotPool, ZeroSlotsDisablesPool)
{
    MaxSlotsGuard guard(0);
    auto & pool = RealTimeSlotPool::instance();

    EXPECT_FALSE(pool.tryAcquire());
    EXPECT_EQ(pool.getUsedSlots(), 0u);
}

TEST(RealTimeSlotPool, LoweringLimitBelowUsageStopsNewAcquisitions)
{
    MaxSlotsGuard guard(2);
    auto & pool = RealTimeSlotPool::instance();

    ASSERT_TRUE(pool.tryAcquire());
    ASSERT_TRUE(pool.tryAcquire());
    ASSERT_EQ(pool.getUsedSlots(), 2u);

    /// Shrinking the limit below the current usage must not let new acquisitions through,
    /// while already-held slots remain valid until released.
    pool.setMaxSlots(1);
    EXPECT_FALSE(pool.tryAcquire());

    pool.release();
    EXPECT_EQ(pool.getUsedSlots(), 1u);
    /// Still at the (lowered) limit.
    EXPECT_FALSE(pool.tryAcquire());

    pool.release();
    EXPECT_EQ(pool.getUsedSlots(), 0u);
}

TEST(OSThreadRealtime, EnableWithoutCapabilityIsSafe)
{
#if defined(OS_LINUX)
    if (hasLinuxCapability(CAP_SYS_NICE))
        GTEST_SKIP() << "Has CAP_SYS_NICE; this test covers the no-capability path";

    /// Without CAP_SYS_NICE, enabling real-time scheduling must fail gracefully (no throw, returns
    /// false) and leave the thread on the default policy.
    EXPECT_FALSE(OSThreadRealtime::enable(1));
    EXPECT_EQ(sched_getscheduler(0), SCHED_OTHER);

    /// disable() must be a safe no-op as well.
    EXPECT_NO_THROW(OSThreadRealtime::disable());
#else
    /// On non-Linux platforms the helpers are no-ops.
    EXPECT_FALSE(OSThreadRealtime::enable(1));
    EXPECT_NO_THROW(OSThreadRealtime::disable());
#endif
}
