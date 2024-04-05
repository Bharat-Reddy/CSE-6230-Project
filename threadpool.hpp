#ifndef THREADPOOL_H
#define THREADPOOL_H
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

// The threadpool is largely based off of online examples but we have added our own optimisations and modifications
// This was allowed as per the faq guilelines.
// Sources: 
// https://github.com/ned14/outcome/blob/5076f63557e0cb4e8e9a7b7150f9da3ad1ba2461/attic/example/io_service_outcome.cpp
// https://nixiz.github.io/yazilim-notlari/2023/10/07/thread_pool-en
// https://www.cnblogs.com/sinkinben/p/16064857.html
// https://github.com/progschj/ThreadPool
class threadpool {
    public:
        // delete copy and move constructors and assign operators
        threadpool(const threadpool&) = delete;
        threadpool& operator=(const threadpool&) = delete;
        threadpool(threadpool&&) = delete;
        threadpool& operator=(threadpool&&) = delete;


        explicit threadpool(std::size_t num_threads = std::thread::hardware_concurrency());
        ~threadpool();

        template <class F, class... Args>
        std::future<typename std::result_of_t<F(Args...)>> enqueue(F&& f, Args&&... args);

        bool queueEmpty()
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            return task_queue.empty();
        }
    private:
        void thread_function();

        // number of threads in the pool
        std::size_t num_threads_;
        // vector of threads
        std::vector<std::thread> worker_threads;
        // queue of tasks
        std::queue<std::function<void()>> task_queue;

        // synchronization variables
        std::mutex queue_mutex;
        std::condition_variable condition;
        bool stop;
};

void threadpool::thread_function()
{
    while (true) 
    {
        std::function<void()> task;
        {
            // lock mutex
            std::unique_lock<std::mutex> lock(this->queue_mutex);

            // wait until stop is set to true or there are tasks in the queue
            // condition.wait() unlocks the mutex and waits for the lambda to return true
            // when the lambda returns true, the mutex is locked again and thread continues
            this->condition.wait(lock, [this] { return this->stop || !this->task_queue.empty(); });

            // if stop is set to true and the queue is empty, return
            if (this->stop && this->task_queue.empty()) 
            {
                return;
            }
            task = std::move(this->task_queue.front());
            this->task_queue.pop();
        }
        task();
    }
}

threadpool::~threadpool()
{
    // set stop flag to true
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        stop = true;
    }

    // notify all threads
    condition.notify_all();

    // join all threads and wait for them to finish if necessary
    for (std::thread& worker : worker_threads) 
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }
}

threadpool::threadpool(size_t num_threads) : stop(false)
{
    num_threads_ = num_threads;  
    // create threads
    worker_threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) 
    {
        // emplace_back thread functions
        std::thread worker(&threadpool::thread_function, this);
        worker_threads.emplace_back(std::move(worker));
    }
}

template <typename F, typename... Args>
std::future<typename std::result_of_t<F(Args...)>> threadpool::enqueue(F&& f, Args&&... args)
{

    // create a task with the given function and arguments
    // https://en.cppreference.com/w/cpp/thread/packaged_task
    auto task = std::make_shared<std::packaged_task<std::result_of_t<F(Args...)>()>>
    (
        std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    // create a future from the task
    // https://en.cppreference.com/w/cpp/types/result_of
    std::future<std::result_of_t<F(Args...)>> result = task->get_future();

    // lock mutex and add task to queue
    {
        std::lock_guard<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if (stop) 
        {
            throw std::runtime_error("enqueue on stopped threadpool");
        }
        task_queue.emplace([task]() { (*task)(); });
    }
    // notify one thread
    condition.notify_one();
    return result;
}
#endif
