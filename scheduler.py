import threading
import time
from priority_inheritance_lock import PriorityInheritanceLock, InheritanceGraph
from priority_ceiling_lock import PriorityCeilingProtocol


MODE_NONE = "none"
MODE_INHERITANCE = "inheritance"
MODE_CEILING = "ceiling"


TASK_READY = "ready"
TASK_RUNNING = "running"
TASK_WAITING = "waiting"
TASK_DONE = "done"


class Task:
    def __init__(self, task_id, base_priority, work_units, lock_ops=None):
        self.task_id = task_id
        self.base_priority = base_priority
        self.work_units = work_units
        self.remaining_work = work_units
        self.lock_ops = lock_ops or []
        self.state = TASK_READY
        self.held_locks = []
        self.waiting_for_lock = None
        self.op_index = 0
        self.blocked_reason = ""
        self.effective_priority = base_priority
        self.start_time = None
        self.finish_time = None
        self.wait_start = None
        self.total_wait_time = 0

    def get_priority(self):
        return self.effective_priority

    def is_done(self):
        return self.state == TASK_DONE

    def __repr__(self):
        return f"Task({self.task_id}, pri={self.base_priority}, eff={self.effective_priority})"


class SimpleLock:
    def __init__(self, name):
        self.name = name
        self.owner = None
        self.waiters = []

    def acquire(self, task_id, priority, timeout=None):
        if self.owner == task_id:
            return "acquired"
        if self.owner is None:
            self.owner = task_id
            return "acquired"
        if task_id not in self.waiters:
            self.waiters.append(task_id)
        return "waiting"

    def release(self, task_id):
        if self.owner != task_id:
            return None
        self.owner = None
        if self.waiters:
            next_id = self.waiters.pop(0)
            self.owner = next_id
            return next_id
        return None


class PriorityScheduler:
    def __init__(self, mode=MODE_NONE, ceiling_config=None):
        self.mode = mode
        self.ceiling_config = ceiling_config or {}
        self.tasks = []
        self.locks = {}
        self.ceiling_protocol = None
        self.inheritance_graph = None
        self.current_time = 0
        self.time_slice = 1
        self._lock = threading.Lock()
        self.event_log = []
        self._init_mode()

    def _init_mode(self):
        if self.mode == MODE_INHERITANCE:
            self.inheritance_graph = InheritanceGraph()

    def register_lock(self, lock_name, ceiling_priority=None):
        with self._lock:
            if self.mode == MODE_NONE:
                self.locks[lock_name] = SimpleLock(lock_name)
            elif self.mode == MODE_INHERITANCE:
                self.locks[lock_name] = PriorityInheritanceLock(
                    lock_name, graph=self.inheritance_graph
                )
            elif self.mode == MODE_CEILING:
                if self.ceiling_protocol is None:
                    self.ceiling_protocol = PriorityCeilingProtocol()
                cp = ceiling_priority or self.ceiling_config.get(lock_name, 10)
                self.locks[lock_name] = self.ceiling_protocol.register_lock(
                    lock_name, cp
                )
            return self.locks[lock_name]

    def add_task(self, task):
        with self._lock:
            self.tasks.append(task)

    def _get_effective_priority(self, task):
        for lock in self.locks.values():
            if hasattr(lock, 'owner_effective_priority') and lock.owner == task.task_id:
                return lock.owner_effective_priority or task.base_priority
        return task.base_priority

    def _update_effective_priorities(self):
        for task in self.tasks:
            task.effective_priority = self._get_effective_priority(task)

    def _get_runnable_tasks(self):
        return [t for t in self.tasks if t.state == TASK_READY]

    def _pick_next_task(self):
        self._update_effective_priorities()
        runnable = self._get_runnable_tasks()
        if not runnable:
            return None
        return max(runnable, key=lambda t: t.effective_priority)

    def _process_lock_op(self, task):
        if task.op_index >= len(task.lock_ops):
            return None

        op = task.lock_ops[task.op_index]
        op_type, lock_name = op[0], op[1]
        lock = self.locks.get(lock_name)

        if lock is None:
            task.op_index += 1
            return None

        if op_type == "acquire":
            result = lock.acquire(task.task_id, task.base_priority, timeout=-1)
            if result == "acquired":
                task.held_locks.append(lock_name)
                task.op_index += 1
                task.waiting_for_lock = None
                if task.wait_start is not None:
                    task.total_wait_time += self.current_time - task.wait_start
                    task.wait_start = None
                self._log(f"任务 {task.task_id} 获取锁 {lock_name}")
                return "acquired"
            elif result == "already_waiting":
                task.state = TASK_WAITING
                task.waiting_for_lock = lock_name
                task.blocked_reason = "already_waiting"
                return result
            elif result in ("waiting", "blocked", "timeout", "cancelled"):
                task.state = TASK_WAITING
                task.waiting_for_lock = lock_name
                task.blocked_reason = result
                if task.wait_start is None:
                    task.wait_start = self.current_time
                self._log(
                    f"任务 {task.task_id} 等待锁 {lock_name}（{result}）"
                )
                return result
        elif op_type == "release":
            if lock_name in task.held_locks:
                lock.release(task.task_id)
                task.held_locks.remove(lock_name)
                task.op_index += 1
                self._log(f"任务 {task.task_id} 释放锁 {lock_name}")

                if hasattr(lock, 'owner') and lock.owner:
                    for t in self.tasks:
                        if t.task_id == lock.owner and t.state == TASK_WAITING and t.waiting_for_lock == lock_name:
                            t.state = TASK_READY
                            t.waiting_for_lock = None
                            t.blocked_reason = ""
                            if t.wait_start is not None:
                                t.total_wait_time += self.current_time - t.wait_start
                                t.wait_start = None
                            self._log(f"任务 {t.task_id} 获得锁 {lock_name}，恢复就绪")
                else:
                    waiters = getattr(lock, 'waiters', {})
                    if waiters:
                        highest = None
                        for tid, entry in waiters.items():
                            for t in self.tasks:
                                if t.task_id == tid and t.state == TASK_WAITING and t.waiting_for_lock == lock_name:
                                    if highest is None or t.base_priority > highest.base_priority:
                                        highest = t
                        if highest:
                            highest.state = TASK_READY
                            highest.waiting_for_lock = None
                            highest.blocked_reason = ""
                            if highest.wait_start is not None:
                                highest.total_wait_time += self.current_time - highest.wait_start
                                highest.wait_start = None
                            self._log(f"任务 {highest.task_id} 获得锁 {lock_name}，恢复就绪")
            return "released"

        return None

    def _log(self, message):
        self.event_log.append((self.current_time, message))

    def _print_status_table(self):
        print(f"\n  [时间片 {self.current_time}] " + "=" * 60)
        header = f"  {'任务':<6}{'基础':<6}{'有效':<6}{'状态':<10}{'持有锁':<15}{'等待锁':<10}{'原因':<15}"
        print(header)
        print("  " + "-" * 68)

        for task in sorted(self.tasks, key=lambda t: -t.base_priority):
            held = ",".join(task.held_locks) if task.held_locks else "-"
            waiting = task.waiting_for_lock or "-"
            reason = task.blocked_reason or "-"
            print(
                f"  {task.task_id:<6}{task.base_priority:<6}{task.effective_priority:<6}"
                f"{task.state:<10}{held:<15}{waiting:<10}{reason:<15}"
            )

        if self.mode == MODE_CEILING and self.ceiling_protocol:
            print(f"\n  天花板状态:")
            for name, lock in self.locks.items():
                owner = lock.owner or "空闲"
                waiters = lock.waiters if hasattr(lock, 'waiters') else []
                w_str = f", 等待者: {waiters}" if waiters else ""
                print(f"    锁 {name}: 天花板={lock.ceiling_priority}, 持有者={owner}{w_str}")
            print(f"    系统天花板 = {self.ceiling_protocol.system_ceiling()}")

        if self.mode == MODE_INHERITANCE and self.inheritance_graph:
            for name, lock in self.locks.items():
                if lock.owner is not None:
                    lock.print_chain("    ")
                    break

        print(f"\n  最近事件:")
        for t, msg in self.event_log[-3:]:
            print(f"    [t={t}] {msg}")

    def step(self, verbose=True):
        with self._lock:
            self.current_time += self.time_slice

            current = self._pick_next_task()
            if current is None:
                if verbose:
                    self._print_status_table()
                return False

            current.state = TASK_RUNNING

            if current.start_time is None:
                current.start_time = self.current_time
                self._log(f"任务 {current.task_id} 开始运行")

            lock_result = self._process_lock_op(current)

            if lock_result in ("waiting", "blocked", "already_waiting", "timeout", "cancelled"):
                current.state = TASK_WAITING
            else:
                current.remaining_work -= self.time_slice
                if current.remaining_work <= 0 and current.op_index >= len(current.lock_ops):
                    current.state = TASK_DONE
                    current.finish_time = self.current_time
                    if current.wait_start is not None:
                        current.total_wait_time += self.current_time - current.wait_start
                        current.wait_start = None
                    self._log(f"任务 {current.task_id} 完成")
                else:
                    current.state = TASK_READY

            if verbose:
                self._print_status_table()

            return True

    def run(self, max_steps=50, verbose=True):
        steps = 0
        while steps < max_steps:
            all_done = all(t.state == TASK_DONE for t in self.tasks)
            if all_done:
                break
            if not self.step(verbose=verbose):
                pass
            steps += 1
            time.sleep(0.01)

        if verbose:
            self._print_summary()

    def _print_summary(self):
        print("\n" + "=" * 70)
        mode_name = {
            MODE_NONE: "无保护",
            MODE_INHERITANCE: "优先级继承",
            MODE_CEILING: "优先级天花板",
        }[self.mode]
        print(f"  模式: {mode_name}")
        print(f"  任务完成情况:")
        for task in sorted(self.tasks, key=lambda t: -t.base_priority):
            start = task.start_time or "-"
            finish = task.finish_time or "-"
            wait = task.total_wait_time
            print(f"    {task.task_id}: 开始={start}, 完成={finish}, 总等待={wait}")
        print("=" * 70)

    def get_summary(self):
        return {
            "mode": self.mode,
            "tasks": [
                {
                    "id": t.task_id,
                    "start": t.start_time,
                    "finish": t.finish_time,
                    "wait_time": t.total_wait_time,
                    "base_priority": t.base_priority,
                }
                for t in self.tasks
            ],
            "total_time": self.current_time,
        }


def run_comparison(lock_ops_by_task, locks_config):
    print("\n" + "=" * 70)
    print("三种模式对比演示")
    print("=" * 70)

    modes = [MODE_NONE, MODE_INHERITANCE, MODE_CEILING]

    results = {}

    for mode in modes:
        print(f"\n{'=' * 70}")
        if mode == MODE_CEILING:
            sched = PriorityScheduler(mode=mode, ceiling_config=locks_config)
        else:
            sched = PriorityScheduler(mode=mode)

        for lock_name, ceiling in locks_config.items():
            sched.register_lock(lock_name, ceiling)

        for task_id, (priority, work, ops) in lock_ops_by_task.items():
            task = Task(task_id, priority, work, ops)
            sched.add_task(task)

        sched.run(max_steps=15, verbose=True)
        results[mode] = sched.get_summary()

    print("\n" + "=" * 70)
    print("对比总结")
    print("=" * 70)
    header = f"{'模式':<12}{'任务':<6}{'基础优先级':<10}{'开始':<6}{'完成':<6}{'等待时间':<8}"
    print(header)
    print("-" * 55)

    mode_names = {
        MODE_NONE: "无保护     ",
        MODE_INHERITANCE: "优先级继承 ",
        MODE_CEILING: "优先级天花板 ",
    }

    for mode in modes:
        summary = results[mode]
        for td in sorted(summary["tasks"], key=lambda x: -x["base_priority"]):
            start = td['start'] if td['start'] is not None else "-"
            finish = td['finish'] if td['finish'] is not None else "-"
            wait = td['wait_time']
            print(
                f"{mode_names[mode]}  {td['id']:<5}    {td['base_priority']:>6}    {start:>4}  {finish:>4}     {wait:>6}"
            )
        print("-" * 55)
