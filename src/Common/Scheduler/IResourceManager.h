#pragma once

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/WorkloadSettings.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <functional>

namespace DB
{

class ISchedulerNode;
using SchedulerNodePtr = std::shared_ptr<ISchedulerNode>;

struct ClassifierSettings
{
    bool throw_on_unknown_workload = false;
};

/*
 * Instance of derived class holds everything required for resource consumption,
 * including resources currently registered at `SchedulerRoot`. This is required to avoid
 * problems during configuration update. Do not hold instances longer than required.
 * Should be created on query start and destructed when query is done.
 */
class IClassifier : private boost::noncopyable
{
public:
    virtual ~IClassifier() = default;

    /// Returns true iff resource access is allowed by this classifier
    virtual bool has(const String & resource_name) = 0;

    /// Returns ResourceLink that should be used to access resource.
    /// Returned link is valid until classifier destruction.
    virtual ResourceLink get(const String & resource_name) = 0;
    /// Returns settings that should be used to limit workload on given resource.
    virtual WorkloadSettings getWorkloadSettings(const String & resource_name) const = 0;
};

using ClassifierPtr = std::shared_ptr<IClassifier>;

/*
 * Represents control plane of resource scheduling. Derived class is responsible for reading
 * configuration, creating all required `ISchedulerNode` objects and
 * managing their lifespan.
 */
class IResourceManager : private boost::noncopyable
{
public:
    virtual ~IResourceManager() = default;

    /// Initialize or reconfigure manager.
    virtual void updateConfiguration(const Poco::Util::AbstractConfiguration & config) = 0;

    virtual void updateConfigurationQueryStart(const String & workload_name) = 0;
    virtual void updateConfigurationQueryEnd(const String & workload_name) = 0;



    /// Returns true iff given resource is controlled through this manager.
    virtual bool hasResource(const String & resource_name) const = 0;

    /// Obtain a classifier instance required to get access to resources.
    /// Note that it holds resource configuration, so should be destructed when query is done.
    virtual ClassifierPtr acquire(const String & classifier_name, const ClassifierSettings & settings) = 0;

    ClassifierPtr acquire(const String & classifier_name)
    {
        return acquire(classifier_name, {});
    }

    /// For introspection, see `system.scheduler` table
    using VisitorFunc = std::function<void(const String & resource, const String & path, ISchedulerNode * node)>;
    virtual void forEachNode(VisitorFunc visitor) = 0;
};

using ResourceManagerPtr = std::shared_ptr<IResourceManager>;

/*
 * RAII guard for tracking workload query count.
 * Automatically increments query count on construction and decrements on destruction.
 * This ensures proper cleanup even when exceptions are thrown.
 */
class WorkloadQueryCountGuard : private boost::noncopyable
{
public:
    WorkloadQueryCountGuard() = default;
 
    WorkloadQueryCountGuard(ResourceManagerPtr manager_, const String & workload_name_)
        : manager(std::move(manager_))
        , workload_name(workload_name_)
        , active(false)
    {
        if (manager && !workload_name.empty() && workload_name != "default")
        {
            manager->updateConfigurationQueryStart(workload_name);
            active = true;
        }
    }
 
    /// Move constructor - transfers ownership
    WorkloadQueryCountGuard(WorkloadQueryCountGuard && other) noexcept
        : manager(std::move(other.manager))
        , workload_name(std::move(other.workload_name))
        , active(other.active)
    {
        other.active = false;
    }
 
     /// Move assignment - transfers ownership
    WorkloadQueryCountGuard & operator=(WorkloadQueryCountGuard && other) noexcept
    {
        if (this != &other)
        {
            release();
            manager = std::move(other.manager);
            workload_name = std::move(other.workload_name);
            active = other.active;
            other.active = false;
        }
        return *this;
    }
 
    ~WorkloadQueryCountGuard()
    {
        release();
    }
 
    /// Manually release the guard (decrements count if active)
    void release()
    {
        if (active && manager)
        {
            manager->updateConfigurationQueryEnd(workload_name);
            active = false;
        }
    }
 
    /// Check if this guard is actively tracking a workload
    bool isActive() const { return active; }
 
private:
    ResourceManagerPtr manager;
    String workload_name;
    bool active = false;
};

}
