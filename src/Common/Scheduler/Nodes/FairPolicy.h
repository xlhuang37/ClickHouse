#pragma once

#include <Common/Scheduler/ISchedulerNode.h>

#include <Common/Stopwatch.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <vector>

namespace ProfileEvents
{
    extern const Event FairPolicyDequeue;
    extern const Event RecomputeWeight;
}

namespace CurrentMetrics
{
    extern const Metric CurrNumQueryClassOne;
    extern const Metric CurrWeightClassOne;
    extern const Metric WhichSpeedUpOne;
    extern const Metric CurrNumQueryClassTwo;
    extern const Metric CurrWeightClassTwo;
    extern const Metric WhichSpeedUpTwo;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

/*
 * Scheduler node that implements weight-based fair scheduling policy.
 * Based on Start-time Fair Queueing (SFQ) algorithm.
 *
 * Algorithm description.
 * Virtual runtime (total consumed cost divided by child weight) is tracked for every child.
 * Active child with minimum vruntime is selected to be dequeued next. On activation, initial vruntime
 * of a child is set to vruntime of "start" of the last request. This guarantees immediate processing
 * of at least single request of newly activated children and thus best isolation and scheduling latency.
 */
class FairPolicy final : public ISchedulerNode
{
    /// Scheduling state of a child
    struct Item
    {
        ISchedulerNode * child = nullptr;
        double vruntime = 0; /// total consumed cost divided by child weight
        double weight = 1.0;

        /// For min-heap by vruntime
        bool operator<(const Item & rhs) const noexcept
        {
            return vruntime > rhs.vruntime;
        }
    };

public:
    explicit FairPolicy(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    FairPolicy(EventQueue * event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerNode(event_queue_, info_)
    {}

    ~FairPolicy() override
    {
        // We need to clear `parent` in all children to avoid dangling references
        while (!children.empty())
            removeChild(children.begin()->second.get());
    }

    const String & getTypeName() const override
    {
        static String type_name("fair");
        return type_name;
    }

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * _ = dynamic_cast<FairPolicy *>(other))
            return true;
        return false;
    }

    void attachChild(const SchedulerNodePtr & child) override
    {
        // Take ownership
        if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Can't add another child with the same path: {}",
                it->second->getPath());

        // Attach
        child->setParent(this);

        // At first attach as inactive child.
        // Inactive attached child must have `info.parent.idx` equal it's index inside `items` array.
        // This is needed to avoid later scanning through inactive `items` in O(N). Important optimization.
        // NOTE: vruntime must be equal to `system_vruntime` for fairness.
        child->info.parent.idx = items.size();
        items.emplace_back(Item{child.get(), system_vruntime});

        // Activate child if it is not empty
        if (child->isActive())
            activateChildImpl(items.size() - 1);
    }

    void removeChild(ISchedulerNode * child) override
    {
        if (auto iter = children.find(child->basename); iter != children.end())
        {
            SchedulerNodePtr removed = iter->second;

            // Deactivate: detach is not very common operation, so we can afford O(N) here
            size_t child_idx = 0;
            [[ maybe_unused ]] bool found = false;
            for (; child_idx != items.size(); child_idx++)
            {
                if (items[child_idx].child == removed.get())
                {
                    found = true;
                    break;
                }
            }
            assert(found);
            if (child_idx < heap_size) // Detach of active child requires deactivation at first
            {
                heap_size--;
                std::swap(items[child_idx], items[heap_size]);
                // Element was removed from inside of heap -- heap must be rebuilt
                std::make_heap(items.begin(), items.begin() + heap_size);
                child_idx = heap_size;
            }

            // Now detach inactive child
            if (child_idx != items.size() - 1)
            {
                std::swap(items[child_idx], items.back());
                items[child_idx].child->info.parent.idx = child_idx;
            }
            items.pop_back();

            // Detach
            removed->setParent(nullptr);

            // Get rid of ownership
            children.erase(iter);
        }
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (auto iter = children.find(child_name); iter != children.end())
            return iter->second.get();
        return nullptr;
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        ProfileEvents::increment(ProfileEvents::FairPolicyDequeue);

        // Periodic weight recompute 
        {
            UInt64 ns = clock_gettime_ns();
            static constexpr UInt64 RECOMPUTE_PERIOD_NS = 1000000000ULL; // 1 second
            if (ns > last_recompute_ns + RECOMPUTE_PERIOD_NS)
            {
                last_recompute_ns = ns;
                ProfileEvents::increment(ProfileEvents::RecomputeWeight);
                recomputeWeights(60);   
            }
        }

        // Cycle is required to do deactivations in the case of canceled requests, when dequeueRequest returns `nullptr`
        while (true)
        {
            if (heap_size == 0)
                return {nullptr, false};

            // Recursively pull request from child
            auto [request, child_active] = items.front().child->dequeueRequest();
            std::pop_heap(items.begin(), items.begin() + heap_size);
            Item & current = items[heap_size - 1];

            if (request)
            {
                // SFQ fairness invariant: system vruntime equals last served request start-time
                assert(current.vruntime >= system_vruntime);
                system_vruntime = current.vruntime;

                // By definition vruntime is amount of consumed resource (cost) divided by weight
                current.vruntime += double(request->cost) / current.weight;
                max_vruntime = std::max(max_vruntime, current.vruntime);
            }

            if (child_active) // Put active child back in heap after vruntime update
            {
                std::push_heap(items.begin(), items.begin() + heap_size);
            }
            else // Deactivate child if it is empty, but remember it's vruntime for latter activations
            {
                heap_size--;

                // Store index of this inactive child in `parent.idx`
                // This enables O(1) search of inactive children instead of O(n)
                current.child->info.parent.idx = heap_size;
            }

            // Reset any difference between children on busy period end
            if (heap_size == 0)
            {
                // Reset vtime to zero to avoid floating-point error accumulation,
                // but do not reset too often, because it's O(N)
                UInt64 ns = clock_gettime_ns();
                if (last_reset_ns + 1000000000 < ns)
                {
                    last_reset_ns = ns;
                    for (Item & item : items)
                        item.vruntime = 0;
                    max_vruntime = 0;
                }
                system_vruntime = max_vruntime;
                busy_periods++;
            }

            if (request)
            {
                incrementDequeued(request->cost);
                return {request, heap_size > 0};
            }
        }
    }

    bool isActive() override
    {
        return heap_size > 0;
    }

    size_t activeChildren() override
    {
        return heap_size;
    }

    void activateChild(ISchedulerNode * child) override
    {
        // Find this child; this is O(1), thanks to inactive index we hold in `parent.idx`
        activateChildImpl(child->info.parent.idx);
    }

    // For introspection
    double getSystemVRuntime() const
    {
        return system_vruntime;
    }

    std::optional<double> getChildVRuntime(ISchedulerNode * child) const
    {
        for (const auto & item : items)
        {
            if (child == item.child)
                return item.vruntime;
        }
        return std::nullopt;
    }

private:
    void activateChildImpl(size_t inactive_idx)
    {
        bool activate_parent = heap_size == 0;

        if (heap_size != inactive_idx)
        {
            std::swap(items[heap_size], items[inactive_idx]);
            items[inactive_idx].child->info.parent.idx = inactive_idx;
        }

        // Newly activated child should have at least `system_vruntime` to keep fairness
        items[heap_size].vruntime = std::max(system_vruntime, items[heap_size].vruntime);
        heap_size++;
        std::push_heap(items.begin(), items.begin() + heap_size);

        // Recursive activation
        if (activate_parent && parent)
            parent->activateChild(this);
    }

    void recomputeWeights(std::size_t total_cores) {
            struct Candidate
            {
                Item * item;          // points into items[]
                std::size_t offset;   // how many "cores" we've given this workload so far
                double weight;
                uint32_t running;     // fixed running query count captured before loop

            };

            std::vector<Candidate> candidates;
            candidates.reserve(heap_size);

            // Build candidate list from active children and capture running queries
            for (std::size_t i = 0; i < heap_size; ++i)
            {
                Item & it = items[i];
                auto & info = it.child->info;

                if (!info.active_speedup)
                    continue;
            
                uint32_t running = info.runtime_stats->running_queries.load(std::memory_order_relaxed);
                if (running == 0)
                    continue;

                candidates.push_back(Candidate{&it, 0, 0.0, running});
            }

            if (candidates.size() >= 1) {
                CurrentMetrics::set(CurrentMetrics::CurrNumQueryClassOne, static_cast<Int64>(candidates[0].running));
            }
            if (candidates.size() >= 2) {
                CurrentMetrics::set(CurrentMetrics::CurrNumQueryClassTwo, static_cast<Int64>(candidates[1].running));
            } 

            if (candidates.empty())
                return;

            // Greedily assign cores one by one
            for (std::size_t core = 0; core < total_cores; ++core)
            {
                Candidate * best = nullptr;
                double best_marginal = 0;

                for (auto & c : candidates)
                {
                    auto & info = c.item->child->info;
                    const auto * speed = info.active_speedup;

                    std::size_t k = std::min<std::size_t>(c.offset, total_cores - 1);
                    k = k / static_cast<size_t>(c.running);

                    // Total speedup at k and k+1 "cores"
                    double s0 = (*speed)[k];
                    double s1 = (*speed)[std::min(k + 1, total_cores - 1)];
                    double marginal = s1 - s0; // Δspeedup for an extra core

                    if (marginal > best_marginal)
                    {
                        best_marginal = marginal;
                        best = &c;
                    }
                }

                if (!best)
                    break;

                // Give this core to the best workload
                best->weight += best_marginal;
                best->offset += 1;
            }

            for (auto & c : candidates)
            {
                c.item->weight = c.weight;
                if (c.item->weight <= 1.0)
                    c.item->weight = 1.0;
            }

            if (candidates.size() >= 1) {
                CurrentMetrics::set(CurrentMetrics::CurrNumQueryClassOne, static_cast<Int64>(candidates[0].running));
                CurrentMetrics::set(CurrentMetrics::CurrWeightClassOne, static_cast<Int64>(candidates[0].item->weight));
                CurrentMetrics::set(CurrentMetrics::WhichSpeedUpOne, static_cast<Int64>(candidates[0].item->child->info.class_index));
            }
            if (candidates.size() >= 2) {
                CurrentMetrics::set(CurrentMetrics::CurrNumQueryClassTwo, static_cast<Int64>(candidates[1].running));
                CurrentMetrics::set(CurrentMetrics::CurrWeightClassTwo, static_cast<Int64>(candidates[1].item->weight + 0.5));
                CurrentMetrics::set(CurrentMetrics::WhichSpeedUpTwo, static_cast<Int64>(candidates[1].item->child->info.class_index));
            } 
        }

        /// Beginning of `items` vector is heap of active children: [0; `heap_size`).
        /// Next go inactive children in unsorted order.
        /// NOTE: we have to track vruntime of inactive children for max-min fairness.
        std::vector<Item> items;
        size_t heap_size = 0;

        /// Last request vruntime
        double system_vruntime = 0;
        double max_vruntime = 0;
        UInt64 last_recompute_ns = 0;   /// last time we recomputed weights
        UInt64 last_reset_ns = 0;

        /// All children with ownership
        std::unordered_map<String, SchedulerNodePtr> children; // basename -> child
    };

}
