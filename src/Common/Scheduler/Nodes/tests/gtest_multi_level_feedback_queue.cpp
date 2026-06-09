#include <gtest/gtest.h>

#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/Nodes/MultiLevelFeedbackQueue.h>
#include <Common/Scheduler/ResourceRequest.h>

#include <limits>

using namespace DB;

namespace
{

/// Minimal request: never actually executed in these tests, we only check queue bookkeeping.
struct TestRequest final : public ResourceRequest
{
    explicit TestRequest(ResourceCost cost_ = 1)
        : ResourceRequest(cost_)
    {}

    void execute() override {}
    void failed(const std::exception_ptr &) override {}
};

/// Helper to build a standalone MLFQ for direct enqueue/dequeue/reprioritize testing.
/// The event queue is never run; enqueue/dequeue are thread-safe and self-contained.
struct QueueFixture
{
    EventQueue event_queue;
    MultiLevelFeedbackQueue queue{&event_queue, SchedulerNodeInfo{}};
};

}

TEST(MultiLevelFeedbackQueue, PickCpuBandIsMonotonicAndClamped)
{
    using MLFQ = MultiLevelFeedbackQueue;

    /// Default finite thresholds (kLayerWidth - 1 entries; the last band is the implicit catch-all).
    const std::vector<ResourceCost> thresholds = {6'296'000'000, 25'004'000'000, 100'016'000'000};
    const auto last_band = static_cast<Priority::Value>(MLFQ::topology().layer_width - 1);

    /// Negative (e.g. arithmetic overflow upstream) and zero both map to the top band.
    EXPECT_EQ(MLFQ::pickCpuBand(-12345, thresholds), 0);
    EXPECT_EQ(MLFQ::pickCpuBand(0, thresholds), 0);

    /// A tiny amount of CPU stays in the top band; an enormous amount falls to the last band.
    EXPECT_EQ(MLFQ::pickCpuBand(1, thresholds), 0);
    EXPECT_EQ(MLFQ::pickCpuBand(std::numeric_limits<ResourceCost>::max(), thresholds), last_band);

    /// Bands are non-decreasing in cumulative CPU and never leave [0, layer_width-1].
    Priority::Value prev = 0;
    for (ResourceCost cpu = 0; cpu < static_cast<ResourceCost>(300'000'000'000); cpu += static_cast<ResourceCost>(1'000'000'000))
    {
        Priority::Value band = MLFQ::pickCpuBand(cpu, thresholds);
        EXPECT_GE(band, 0);
        EXPECT_LE(band, last_band);
        EXPECT_GE(band, prev);
        prev = band;
    }
}

TEST(MultiLevelFeedbackQueue, ParseDemotionThresholds)
{
    const auto defaults = CPULeaseSettings::default_demotion_thresholds_ns();

    /// Well-formed list (with surrounding whitespace) parses verbatim.
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds(" 100 , 200 ,300"),
              (std::vector<ResourceCost>{100, 200, 300}));

    /// Empty and whitespace-only fall back to defaults.
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds(""), defaults);
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds("   "), defaults);

    /// Malformed, negative, and non-monotonic inputs fall back to defaults.
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds("abc"), defaults);
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds("100,foo,300"), defaults);
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds("-5,100"), defaults);
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds("300,200,100"), defaults);

    /// Equal consecutive values are allowed (non-decreasing).
    EXPECT_EQ(CPULeaseSettings::parseDemotionThresholds("100,100,200"),
              (std::vector<ResourceCost>{100, 100, 200}));
}

TEST(MultiLevelFeedbackQueue, LevelingOrdersLowerLevelFirst)
{
    QueueFixture f;

    /// A high-parallelism query lives in a higher (lower-priority) layer than a low-parallelism one.
    TestRequest high_parallelism; // e.g. layer 1
    TestRequest low_parallelism;  // e.g. layer 0

    /// Enqueue the lower-priority (higher level value) request first to prove ordering is by
    /// priority, not arrival order.
    f.queue.enqueueRequest(&high_parallelism, Priority{static_cast<Priority::Value>(1 + MultiLevelFeedbackQueue::topology().layer_width)});
    f.queue.enqueueRequest(&low_parallelism, Priority{1});

    auto [first, has_more1] = f.queue.dequeueRequest();
    EXPECT_EQ(first, &low_parallelism);
    EXPECT_TRUE(has_more1);

    auto [second, has_more2] = f.queue.dequeueRequest();
    EXPECT_EQ(second, &high_parallelism);
    EXPECT_FALSE(has_more2);
}

TEST(MultiLevelFeedbackQueue, FifoWithinLevel)
{
    QueueFixture f;

    TestRequest a;
    TestRequest b;
    TestRequest c;
    f.queue.enqueueRequest(&a, Priority{3});
    f.queue.enqueueRequest(&b, Priority{3});
    f.queue.enqueueRequest(&c, Priority{3});

    EXPECT_EQ(f.queue.dequeueRequest().first, &a);
    EXPECT_EQ(f.queue.dequeueRequest().first, &b);
    EXPECT_EQ(f.queue.dequeueRequest().first, &c);
}

TEST(MultiLevelFeedbackQueue, ReprioritizePromotesPendingRequest)
{
    QueueFixture f;

    /// Two requests in the same low-priority layer. `pending` simulates a query whose parallelism
    /// just dropped: it should be promoted to a higher layer ahead of `other`.
    TestRequest pending;
    TestRequest other;
    f.queue.enqueueRequest(&pending, Priority{5});
    f.queue.enqueueRequest(&other, Priority{5});

    /// Promote `pending` to a strictly higher priority (lower level) than `other`.
    EXPECT_TRUE(f.queue.reprioritizeRequest(&pending, Priority{1}));

    /// Now the promoted request must come out first despite being enqueued first at equal priority.
    EXPECT_EQ(f.queue.dequeueRequest().first, &pending);
    EXPECT_EQ(f.queue.dequeueRequest().first, &other);
}

TEST(MultiLevelFeedbackQueue, ReprioritizeDemotesPendingRequest)
{
    QueueFixture f;

    TestRequest first_in;
    TestRequest second_in;
    f.queue.enqueueRequest(&first_in, Priority{2});
    f.queue.enqueueRequest(&second_in, Priority{4});

    /// Demote `first_in` below `second_in`; ordering must flip.
    EXPECT_TRUE(f.queue.reprioritizeRequest(&first_in, Priority{6}));

    EXPECT_EQ(f.queue.dequeueRequest().first, &second_in);
    EXPECT_EQ(f.queue.dequeueRequest().first, &first_in);
}

TEST(MultiLevelFeedbackQueue, ReprioritizeSameLevelIsNoop)
{
    QueueFixture f;

    TestRequest a;
    TestRequest b;
    f.queue.enqueueRequest(&a, Priority{3});
    f.queue.enqueueRequest(&b, Priority{3});

    EXPECT_TRUE(f.queue.reprioritizeRequest(&a, Priority{3}));

    EXPECT_EQ(f.queue.dequeueRequest().first, &a);
    EXPECT_EQ(f.queue.dequeueRequest().first, &b);
}

TEST(MultiLevelFeedbackQueue, ReprioritizeUnknownOrDequeuedReturnsFalse)
{
    QueueFixture f;

    TestRequest enqueued;
    TestRequest never_enqueued;
    f.queue.enqueueRequest(&enqueued, Priority{2});

    /// Unknown request -> no-op false (mirrors cancelRequest contract for foreign requests is an
    /// exception, but reprioritize is deliberately lenient and just reports false).
    EXPECT_FALSE(f.queue.reprioritizeRequest(&never_enqueued, Priority{1}));

    /// After the request is dequeued (granted), reprioritize must report false (too late).
    EXPECT_EQ(f.queue.dequeueRequest().first, &enqueued);
    EXPECT_FALSE(f.queue.reprioritizeRequest(&enqueued, Priority{1}));
}
