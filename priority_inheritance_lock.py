import threading
from collections import defaultdict


class PriorityInheritanceLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._owner = None
        self._owner_original_priority = None
        self._waiters = []
        self._priority_map = {}

    def acquire(self, task_id, priority):
        self._priority_map[task_id] = priority

        with self._lock:
            if self._owner is None:
                self._owner = task_id
                self._owner_original_priority = priority
                return True

            self._waiters.append((task_id, priority))
            self._waiters.sort(key=lambda x: -x[1])

            if priority > self._owner_original_priority:
                self._promote_owner(priority)

            return False

    def release(self, task_id):
        with self._lock:
            if self._owner != task_id:
                raise RuntimeError(f"Task {task_id} does not own the lock")

            if self._waiters:
                next_task, next_priority = self._waiters.pop(0)
                self._owner = next_task
                self._owner_original_priority = self._priority_map[next_task]

                if self._waiters:
                    highest_waiter_priority = self._waiters[0][1]
                    if highest_waiter_priority > self._owner_original_priority:
                        self._promote_owner(highest_waiter_priority)
            else:
                self._owner = None
                self._owner_original_priority = None

    def _promote_owner(self, new_priority):
        old_priority = self._owner_original_priority
        self._owner_original_priority = new_priority
        print(f"  [继承] 任务 {self._owner} 优先级从 {old_priority} 提升到 {new_priority}")

    @property
    def owner(self):
        return self._owner

    @property
    def owner_effective_priority(self):
        return self._owner_original_priority


class PriorityScheduler:
    def __init__(self):
        self._tasks = {}
        self._lock = threading.Lock()
        self._current_task = None

    def add_task(self, task_id, priority, func):
        self._tasks[task_id] = {
            'priority': priority,
            'func': func,
            'state': 'ready'
        }

    def run(self):
        ready_tasks = sorted(
            [(tid, info) for tid, info in self._tasks.items() if info['state'] == 'ready'],
            key=lambda x: -x[1]['priority']
        )

        if not ready_tasks:
            return

        current_id, current_info = ready_tasks[0]
        self._current_task = current_id
        current_info['state'] = 'running'

        print(f"\n[调度] 运行任务 {current_id} (优先级 {current_info['priority']})")
        current_info['func']()
        current_info['state'] = 'done'
        print(f"[调度] 任务 {current_id} 完成")
