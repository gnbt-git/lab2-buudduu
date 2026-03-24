#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

/*
 * TaskSystemSerial
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    int num_threads_;
};

/*
 * TaskSystemParallelSpawn
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    int num_threads_;
};

/*
 * TaskSystemParallelThreadPoolSpinning
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    void workerLoop();

    int num_threads_;
    std::vector<std::thread> workers_;

    std::mutex mutex_;
    IRunnable *current_runnable_;
    int current_total_tasks_;
    int next_task_;
    std::atomic<int> finished_tasks_;
    bool has_work_;
    bool shutdown_;
};

/*
 * TaskSystemParallelThreadPoolSleeping
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    struct BulkTask
    {
        TaskID id;
        IRunnable *runnable;
        int num_total_tasks;
        std::vector<TaskID> deps;
        std::vector<TaskID> dependents;
        std::atomic<int> next_subtask;
        std::atomic<int> remaining_subtasks;
        int unresolved_deps;
        bool completed;

        BulkTask(TaskID task_id, IRunnable *r, int total,
                 const std::vector<TaskID> &dependencies)
            : id(task_id),
              runnable(r),
              num_total_tasks(total),
              deps(dependencies),
              next_subtask(0),
              remaining_subtasks(total),
              unresolved_deps(0),
              completed(false)
        {
        }
    };

    void workerLoop();
    void completeBulkTask(const std::shared_ptr<BulkTask> &task);

    int num_threads_;
    std::vector<std::thread> workers_;

    std::mutex mutex_;
    std::condition_variable cv_work_;
    std::condition_variable cv_done_;
    bool shutdown_;

    TaskID next_task_id_;
    int unfinished_bulk_tasks_;

    std::deque<TaskID> ready_queue_;
    std::unordered_map<TaskID, std::shared_ptr<BulkTask>> tasks_;
};

#endif