#include "tasksys.h"

#include <algorithm>
#include <thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    (void)deps;
    run(runnable, num_total_tasks);
    return 0;
}

void TaskSystemSerial::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Spawn Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads)
{
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
        return;

    int worker_count = std::max(1, std::min(num_threads_, num_total_tasks));
    std::vector<std::thread> threads;
    threads.reserve(worker_count);

    int chunk = (num_total_tasks + worker_count - 1) / worker_count;

    for (int t = 0; t < worker_count; t++)
    {
        int start = t * chunk;
        int end = std::min(start + chunk, num_total_tasks);

        threads.emplace_back([=]()
                             {
                                 for (int i = start; i < end; i++)
                                 {
                                     runnable->runTask(i, num_total_tasks);
                                 } });
    }

    for (auto &th : threads)
    {
        th.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    (void)deps;
    run(runnable, num_total_tasks);
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(std::max(1, num_threads)),
      current_runnable_(nullptr),
      current_total_tasks_(0),
      next_task_(0),
      finished_tasks_(0),
      has_work_(false),
      shutdown_(false)
{
    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++)
    {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
        has_work_ = false;
    }

    for (auto &worker : workers_)
    {
        if (worker.joinable())
            worker.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop()
{
    while (true)
    {
        IRunnable *runnable = nullptr;
        int total_tasks = 0;
        int task_id = -1;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (shutdown_)
                return;

            if (has_work_ && next_task_ < current_total_tasks_)
            {
                task_id = next_task_++;
                runnable = current_runnable_;
                total_tasks = current_total_tasks_;
            }
        }

        if (task_id != -1)
        {
            runnable->runTask(task_id, total_tasks);
            int done = finished_tasks_.fetch_add(1) + 1;

            if (done == total_tasks)
            {
                std::lock_guard<std::mutex> lock(mutex_);
                has_work_ = false;
            }
        }
        else
        {
            std::this_thread::yield();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
        return;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        current_runnable_ = runnable;
        current_total_tasks_ = num_total_tasks;
        next_task_ = 0;
        finished_tasks_.store(0);
        has_work_ = true;
    }

    while (finished_tasks_.load() < num_total_tasks)
    {
        std::this_thread::yield();
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        current_runnable_ = nullptr;
        current_total_tasks_ = 0;
        next_task_ = 0;
        has_work_ = false;
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    (void)deps;
    run(runnable, num_total_tasks);
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(std::max(1, num_threads)),
      shutdown_(false),
      next_task_id_(0),
      unfinished_bulk_tasks_(0)
{
    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++)
    {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    sync();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    cv_work_.notify_all();

    for (auto &worker : workers_)
    {
        if (worker.joinable())
            worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    std::shared_ptr<BulkTask> new_task;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        TaskID id = next_task_id_++;
        new_task = std::make_shared<BulkTask>(id, runnable, num_total_tasks, deps);

        unfinished_bulk_tasks_++;
        tasks_[id] = new_task;

        for (TaskID dep_id : deps)
        {
            auto it = tasks_.find(dep_id);
            if (it != tasks_.end() && !it->second->completed)
            {
                new_task->unresolved_deps++;
                it->second->dependents.push_back(id);
            }
        }

        if (num_total_tasks == 0)
        {
            new_task->completed = true;
            unfinished_bulk_tasks_--;

            for (TaskID child_id : new_task->dependents)
            {
                auto child_it = tasks_.find(child_id);
                if (child_it != tasks_.end())
                {
                    child_it->second->unresolved_deps--;
                    if (child_it->second->unresolved_deps == 0)
                    {
                        ready_queue_.push_back(child_id);
                    }
                }
            }

            cv_done_.notify_all();
            cv_work_.notify_all();
            return id;
        }

        if (new_task->unresolved_deps == 0)
        {
            ready_queue_.push_back(id);
            cv_work_.notify_all();
        }

        return id;
    }
}

void TaskSystemParallelThreadPoolSleeping::completeBulkTask(const std::shared_ptr<BulkTask> &task)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (task->completed)
        return;

    task->completed = true;
    unfinished_bulk_tasks_--;

    for (TaskID child_id : task->dependents)
    {
        auto it = tasks_.find(child_id);
        if (it != tasks_.end())
        {
            it->second->unresolved_deps--;
            if (it->second->unresolved_deps == 0)
            {
                ready_queue_.push_back(child_id);
            }
        }
    }

    cv_work_.notify_all();
    cv_done_.notify_all();
}

void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        std::shared_ptr<BulkTask> task;
        int subtask_id = -1;

        {
            std::unique_lock<std::mutex> lock(mutex_);

            cv_work_.wait(lock, [this]()
                          { return shutdown_ || !ready_queue_.empty(); });

            if (shutdown_ && ready_queue_.empty())
                return;

            while (!ready_queue_.empty())
            {
                TaskID front_id = ready_queue_.front();
                auto it = tasks_.find(front_id);

                if (it == tasks_.end() || it->second->completed)
                {
                    ready_queue_.pop_front();
                    continue;
                }

                task = it->second;
                subtask_id = task->next_subtask.fetch_add(1);

                if (subtask_id >= task->num_total_tasks)
                {
                    ready_queue_.pop_front();
                    task.reset();
                    subtask_id = -1;
                    continue;
                }

                if (subtask_id + 1 >= task->num_total_tasks)
                {
                    ready_queue_.pop_front();
                }

                break;
            }
        }

        if (!task || subtask_id < 0)
            continue;

        task->runnable->runTask(subtask_id, task->num_total_tasks);

        int left = task->remaining_subtasks.fetch_sub(1) - 1;
        if (left == 0)
        {
            completeBulkTask(task);
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    std::unique_lock<std::mutex> lock(mutex_);
    cv_done_.wait(lock, [this]()
                  { return unfinished_bulk_tasks_ == 0; });
}