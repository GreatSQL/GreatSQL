/* Copyright (c) 2024, GreatDB Software Co., Ltd.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class Thread_pool {
 public:
  Thread_pool(size_t size) {
    workers.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      workers.emplace_back([this, i] {
        std::function<void(size_t)> task;
        while (true) {
          {
            std::unique_lock<std::mutex> lock(this->mutex);
            this->cond.wait(
                lock, [this] { return !this->queue.empty() || this->stop; });
            if (this->stop && this->queue.empty()) break;
            task = std::move(this->queue.front());
            this->queue.pop();
          }
          task(i);
        }
      });
    }
  }

  std::future<void> add_task(std::function<void(size_t)> &&f) {
    auto task = std::make_shared<std::packaged_task<void(size_t)>>(
        std::bind(f, std::placeholders::_1));
    {
      std::lock_guard<std::mutex> lock(mutex);
      queue.emplace([task](size_t i) { (*task)(i); });
    }
    cond.notify_one();
    return task->get_future();
  }

  ~Thread_pool() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      stop = true;
    }
    cond.notify_all();
    for (auto &worker : workers) worker.join();
  }

 private:
  std::vector<std::thread> workers;
  std::mutex mutex;
  std::condition_variable cond;
  std::queue<std::function<void(size_t)>> queue;
  bool stop{false};
};

#endif
