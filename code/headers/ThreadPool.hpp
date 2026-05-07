#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>

class ThreadPool {
    public:
        ThreadPool(size_t threads);
        ~ThreadPool();

        template<class F, class... Args>
        auto enqueue(F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>>
        {
            // Usamos el alias para que sea más legible para el compilador
            using return_type = std::invoke_result_t<F, Args...>;

            auto task = std::make_shared<std::packaged_task<return_type()>>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...)
            );

            std::future<return_type> res = task->get_future();
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                if (stop) throw std::runtime_error("enqueue en ThreadPool detenido");

                // El truco está en encapsular la ejecución dentro de una lambda void
                tasks.emplace([task]() { (*task)(); });
            }
            condition.notify_one();
            return res;
        }


    private:
        std::vector<std::thread> workers;
        std::queue<std::function<void()>> tasks;
        std::mutex queue_mutex;
        std::condition_variable condition;
        bool stop;
};
