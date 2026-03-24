#include <iostream>
#include <vector>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <thread>
#include "tasksys.h"

// Тооцоолол хийх "Ажил" (Task) класс
class ComputeTask : public IRunnable
{
public:
    std::vector<double> results;
    int workload_intensity;

    ComputeTask(int num_tasks, int intensity)
        : results(num_tasks, 0.0), workload_intensity(intensity) {}

    void runTask(int taskID, int num_total_tasks) override
    {
        (void)num_total_tasks;

        double val = 0.0;
        for (int i = 0; i < workload_intensity; ++i)
        {
            val += std::sin(i * 0.01 + taskID) * std::cos(i * 0.02 + taskID);
        }
        results[taskID] = val;
    }
};

void runBenchmark(ITaskSystem *system, IRunnable *task, int num_tasks, const std::string &name)
{
    const int repeats = 5;
    double total = 0.0;

    std::cout << "Testing [" << name << "]..." << std::flush;

    for (int r = 0; r < repeats; r++)
    {
        auto start = std::chrono::high_resolution_clock::now();
        system->run(task, num_tasks);
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> elapsed = end - start;
        total += elapsed.count();
    }

    std::cout << " Done. Avg Time: "
              << std::fixed << std::setprecision(6)
              << (total / repeats) << "s" << std::endl;
}

int main()
{
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0)
        num_threads = 4;

    int num_tasks = 50000;
    int workload_intensity = 2000;

    std::cout << "========================================" << std::endl;
    std::cout << "Task System Benchmark" << std::endl;
    std::cout << "Threads: " << num_threads
              << ", Tasks: " << num_tasks
              << ", Intensity: " << workload_intensity << std::endl;
    std::cout << "========================================" << std::endl;

    {
        ComputeTask task(num_tasks, workload_intensity);
        ITaskSystem *serialSystem = new TaskSystemSerial(num_threads);
        runBenchmark(serialSystem, &task, num_tasks, "Serial System");
        delete serialSystem;
    }

    {
        ComputeTask task(num_tasks, workload_intensity);
        ITaskSystem *spawnSystem = new TaskSystemParallelSpawn(num_threads);
        runBenchmark(spawnSystem, &task, num_tasks, "Parallel Spawn");
        delete spawnSystem;
    }

    {
        ComputeTask task(num_tasks, workload_intensity);
        ITaskSystem *spinningSystem = new TaskSystemParallelThreadPoolSpinning(num_threads);
        runBenchmark(spinningSystem, &task, num_tasks, "Parallel Spinning Pool");
        delete spinningSystem;
    }

    {
        ComputeTask task(num_tasks, workload_intensity);
        ITaskSystem *sleepingSystem = new TaskSystemParallelThreadPoolSleeping(num_threads);
        runBenchmark(sleepingSystem, &task, num_tasks, "Parallel Sleeping Pool");
        delete sleepingSystem;
    }

    std::cout << "========================================" << std::endl;
    std::cout << "Benchmark finished." << std::endl;
    std::cout << "Expected trend: Serial > Spawn > Spinning > Sleeping" << std::endl;
    std::cout << "========================================" << std::endl;
{
    std::cout << "\n=== Dependency Test ===" << std::endl;

    ComputeTask taskA(100, 1000);
    ComputeTask taskB(100, 1000);
    ComputeTask taskC(100, 1000);

    ITaskSystem *system = new TaskSystemParallelThreadPoolSleeping(num_threads);

    TaskID A = system->runAsyncWithDeps(&taskA, 100, {});
    TaskID B = system->runAsyncWithDeps(&taskB, 100, {A});
    TaskID C = system->runAsyncWithDeps(&taskC, 100, {A});

    system->sync();

    std::cout << "Dependency tasks finished!" << std::endl;

    delete system;
}
    return 0;
}