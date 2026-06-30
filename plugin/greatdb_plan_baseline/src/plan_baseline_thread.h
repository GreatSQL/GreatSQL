/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#ifndef PLUGIN_GDB_PLAN_BASELINE_TREAHD_INCLUDED
#define PLUGIN_GDB_PLAN_BASELINE_TREAHD_INCLUDED
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <stdexcept>
#include <vector>
#include "mysql/components/services/bits/my_thread_bits.h"
#include "mysql/components/services/bits/mysql_mutex_bits.h"
#include "mysql/components/services/bits/psi_mutex_bits.h"
#include "sql/sql_class.h"

// table: sql_plan_baselines, hist_sql_plan, digest_sql_info
#define PLAN_BASELINE_TABLE_COUNT 3

namespace greatdb_plan_baseline {

class WorkerThread {
  std::function<void()> m_run;

 public:
  WorkerThread(std::function<void()> f)
      : m_run(f), m_run_once(false), m_stop(false) {}
  virtual ~WorkerThread() {}
  inline int create_thread(PSI_thread_key background_psi_thread_key = 0) {
    int err = mysql_thread_create(background_psi_thread_key, &m_handle, nullptr,
                                  thread_func, this);
    // wait for the thread running
    if (err == 0) {
      while (!m_run_once) {
        usleep(200);  // yield CPU to let new thread do THD::init
      }
    }
    return err;
  }
  inline int join() { return my_thread_join(&m_handle, nullptr); }

  inline void set_stop(bool b) { m_stop = b; }
  inline bool is_stop() { return m_stop; }

 private:
  static void *thread_func(void *const thread_ptr) {
    assert(thread_ptr != nullptr);
    WorkerThread *const thread = static_cast<WorkerThread *>(thread_ptr);
    if (!thread->m_run_once.exchange(true)) {
      // init THD for SIG handler
      my_thread_init();
      // gdb_backend_thread = true;
      THD *thd;
#ifdef EXTRA_CODE_FOR_UNIT_TESTING
      if (!(thd = new (std::nothrow) THD(false))) {
#else
      if (!(thd = new (std::nothrow) THD)) {
#endif
        abort();
        return nullptr;
      }
      // must use local thread variables for stackoverflow check
      thd->thread_stack = (char *)&thd;
      thd->store_globals();
      thread->m_thd = thd;

      thread->m_run();
      thread->set_stop(true);

      // clear THD
      delete thd;
      thread->m_thd = thd = nullptr;
      my_thread_end();
    }
    return nullptr;
  }
  // Disable Copying
  WorkerThread(const WorkerThread &);
  WorkerThread &operator=(const WorkerThread &);
  my_thread_handle m_handle;

  THD *m_thd;

 protected:
  // Make sure we run only once
  std::atomic_bool m_run_once;
  bool m_stop;
};

class Gdb_create_plan_tables : public WorkerThread {
 public:
  Gdb_create_plan_tables(ulong ticker, std::function<void()> init_func,
                         std::function<void()> run_func)
      : WorkerThread(std::bind(&Gdb_create_plan_tables::run, this)),
        m_mutex_inited(false),
        m_ticker(ticker),
        m_init(init_func),
        m_create(run_func),
        m_enable_persistent(false) {}

  virtual ~Gdb_create_plan_tables() { uninit(); }

  inline void init(PSI_mutex_key stop_bg_psi_mutex_key = 0,
                   PSI_cond_key stop_bg_psi_cond_key = 0) {
    assert(!m_run_once);
    mysql_mutex_init(stop_bg_psi_mutex_key, &m_signal_mutex,
                     MY_MUTEX_INIT_FAST);
    mysql_cond_init(stop_bg_psi_cond_key, &m_signal_cond);
    m_mutex_inited = true;
  }

  inline void signal(const bool &stop_thread = false) {
    if (stop_thread && m_stop) return;

    mysql_mutex_lock(&m_signal_mutex);
    if (stop_thread) {
      m_stop = true;
    }
    mysql_cond_signal(&m_signal_cond);
    mysql_mutex_unlock(&m_signal_mutex);
  }
  inline void uninit() {
    if (m_mutex_inited) {
      mysql_mutex_destroy(&m_signal_mutex);
      mysql_cond_destroy(&m_signal_cond);
      m_mutex_inited = false;
    }
  }
  inline void UpdateTc(ulong ticker) { m_ticker.store(ticker); }
  inline void Update_enable_persistent(bool enable) {
    m_enable_persistent.store(enable);
  }

 private:
  inline void run(void) {
    if (m_init) m_init();
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += m_ticker.load();
    while (true) {
      if (m_stop) {
        break;
      }
      if (m_enable_persistent.load()) {
        mysql_mutex_lock(&m_signal_mutex);
        if (m_stop) {
          mysql_mutex_unlock(&m_signal_mutex);
          break;
        }
        const auto ret MY_ATTRIBUTE((__unused__)) =
            mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts);
        if (m_stop) {
          mysql_mutex_unlock(&m_signal_mutex);
          break;
        }
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += m_ticker.load();  // seconds
        mysql_mutex_unlock(&m_signal_mutex);
        if (m_create) m_create();
      } else {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += m_ticker.load();  // seconds
      }
    }
  }
  // Disable Copying
  Gdb_create_plan_tables(const Gdb_create_plan_tables &);
  Gdb_create_plan_tables &operator=(const Gdb_create_plan_tables &);
  bool m_mutex_inited;

 protected:
  mysql_mutex_t m_signal_mutex;
  mysql_cond_t m_signal_cond;
  std::atomic<ulong> m_ticker;
  std::function<void()> m_init;
  std::function<void()> m_create;
  std::atomic<bool> m_enable_persistent;
};

class Mutex_guard {
 public:
  Mutex_guard(mysql_mutex_t &mutex) : m_mutex(mutex) {
    mysql_mutex_lock(&m_mutex);
  }
  ~Mutex_guard() { mysql_mutex_unlock(&m_mutex); }

 private:
  mysql_mutex_t &m_mutex;
};

/**
 * @brief base c++17 Task pool
 *
 */
class ThreadPool {
 public:
  explicit ThreadPool(PSI_thread_key psi_thread_key = 0,
                      PSI_mutex_key queue_mutex_key = 0,
                      PSI_mutex_key in_flight_mutex_key = 0,
                      PSI_cond_key condition_producers_key = 0,
                      PSI_cond_key condition_consumers_key = 0,
                      PSI_cond_key in_flight_condition_key = 0);

  template <class F, class... Args>
  auto enqueue(F &&f, Args &&...args) -> std::future<
      typename std::invoke_result<F &&, std::size_t, Args &&...>::type>;

  void wait_until_empty();
  void wait_until_nothing_in_flight();

  std::size_t get_pool_size() {
    Mutex_guard guard(queue_mutex);
    return pool_size;
  }
  ~ThreadPool();

 private:
  void run(std::size_t worker_number);

  void start_worker(PSI_thread_key psi_thread_key, std::size_t worker_number);

  // need to keep track of threads so we can join them
  std::vector<std::unique_ptr<WorkerThread>> workers;

  // lock queue
  mysql_mutex_t queue_mutex;
  // the task queue
  std::queue<std::function<void(std::size_t)>> tasks;

  mysql_cond_t condition_producers;
  mysql_cond_t condition_consumers;
  mysql_mutex_t in_flight_mutex;
  mysql_cond_t in_flight_condition;

  // target pool size
  std::size_t pool_size;

  // stop signal
  bool stop = false;

  std::atomic<std::size_t> in_flight;

  PSI_thread_key m_psi_thread_key;
  // queue length limit awr max work
  std::size_t max_queue_size;

  struct handle_in_flight_decrement {
    ThreadPool &tp;

    handle_in_flight_decrement(ThreadPool &tp_) : tp(tp_) {}

    ~handle_in_flight_decrement() {
      //
      std::size_t prev = std::atomic_fetch_sub_explicit(
          &tp.in_flight, std::size_t(1), std::memory_order_acq_rel);
      if (prev == 1) {
        Mutex_guard guard(tp.in_flight_mutex);
        mysql_cond_broadcast(&tp.in_flight_condition);
      }
    }
  };
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(PSI_thread_key psi_thread_key,
                              PSI_mutex_key queue_mutex_key,
                              PSI_mutex_key in_flight_mutex_key,
                              PSI_cond_key condition_producers_key,
                              PSI_cond_key condition_consumers_key,
                              PSI_cond_key in_flight_condition_key)
    : pool_size(PLAN_BASELINE_TABLE_COUNT),
      stop(false),
      in_flight(0),
      m_psi_thread_key(psi_thread_key),
      max_queue_size(128) {
  mysql_mutex_init(queue_mutex_key, &queue_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(in_flight_mutex_key, &in_flight_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(condition_producers_key, &condition_producers);
  mysql_cond_init(condition_consumers_key, &condition_consumers);
  mysql_cond_init(in_flight_condition_key, &in_flight_condition);

  Mutex_guard lock(queue_mutex);
  for (std::size_t i = 0; i != pool_size; ++i)
    start_worker(m_psi_thread_key, i);
}

// add new work item to the pool
template <class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&...args) -> std::future<
    typename std::invoke_result<F &&, std::size_t, Args &&...>::type> {
  using return_type =
      typename std::invoke_result<F &&, std::size_t, Args &&...>::type;

  auto task = std::make_shared<std::packaged_task<return_type(std::size_t)>>(
      std::bind(std::forward<F>(f), std::placeholders::_1,
                std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();

  Mutex_guard guard(queue_mutex);
  if (tasks.size() >= max_queue_size) {
    // wait for the queue to empty or be stopped
    while (tasks.size() > max_queue_size && !stop) {
      mysql_cond_wait(&condition_producers, &queue_mutex);
    }
  }

  if (stop) throw std::runtime_error("ThreadPool is stopping");

  tasks.emplace([task](std::size_t n) { (*task)(n); });
  // i++;
  std::atomic_fetch_add_explicit(&in_flight, std::size_t(1),
                                 std::memory_order_relaxed);
  // to run
  mysql_cond_signal(&condition_consumers);
  return res;
}

inline ThreadPool::~ThreadPool() {
  mysql_mutex_lock(&queue_mutex);
  if (!stop) {
    stop = true;
    pool_size = 0;
    mysql_cond_broadcast(&condition_consumers);
    mysql_cond_broadcast(&condition_producers);
  }
  mysql_mutex_unlock(&queue_mutex);

  for (auto &worker : workers) {
    worker->join();
  }
  mysql_mutex_destroy(&queue_mutex);
  assert(in_flight == 0);
  mysql_mutex_destroy(&in_flight_mutex);
  mysql_cond_destroy(&condition_producers);
  mysql_cond_destroy(&condition_consumers);
  mysql_cond_destroy(&in_flight_condition);
}

inline void ThreadPool::wait_until_empty() {
  Mutex_guard guard(queue_mutex);
  while (!tasks.empty()) {
    mysql_cond_wait(&condition_producers, &queue_mutex);
  }
}

inline void ThreadPool::wait_until_nothing_in_flight() {
  Mutex_guard guard(in_flight_mutex);
  while (in_flight != 0) {
    mysql_cond_wait(&in_flight_condition, &in_flight_mutex);
  }
}

inline void ThreadPool::run(std::size_t worker_number) {
  while (true) {
    std::function<void(std::size_t)> task;
    bool notify;

    {
      Mutex_guard lock(queue_mutex);
      // wait
      while (!stop && tasks.empty() && pool_size > worker_number) {
        mysql_cond_wait(&condition_consumers, &queue_mutex);
      }

      // deal with downsizing of thread pool or shutdown
      if ((stop && tasks.empty()) || (!stop && pool_size <= worker_number)) {
        // mysql do not detach api .... orz

        return;
      } else if (!tasks.empty()) {
        // maybe both have task and stop ,to do task and then stop
        task = std::move(tasks.front());
        tasks.pop();
        notify = tasks.size() + 1 == max_queue_size || tasks.empty();
      } else
        continue;
    }
    handle_in_flight_decrement guard(*this);
    if (notify) {
      Mutex_guard lock(queue_mutex);
      mysql_cond_broadcast(&condition_producers);
    }
    task(worker_number);
  }
}

inline void ThreadPool::start_worker(PSI_thread_key psi_thread_key,
                                     std::size_t worker_number) {
  // in lock
  assert(worker_number >= workers.size());

  std::unique_ptr<WorkerThread> task_thread(
      new WorkerThread(std::bind(&ThreadPool::run, this, worker_number)));
  if (task_thread->create_thread(psi_thread_key) != 0) {
    return;
  }
  this->workers.push_back(std::move(task_thread));
}

}  // namespace greatdb_plan_baseline

#endif  // GDB_TREAHD_INCLUDED
