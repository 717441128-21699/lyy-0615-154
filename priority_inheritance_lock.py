import threading
from collections import OrderedDict


ACQUIRE_OK = "acquired"
ACQUIRE_TIMEOUT = "timeout"
ACQUIRE_CANCELLED = "cancelled"


class WaiterEntry:
    __slots__ = ("task_id", "priority", "event", "result")

    def __init__(self, task_id, priority):
        self.task_id = task_id
        self.priority = priority
        self.event = threading.Event()
        self.result = None


class PriorityInheritanceLock:
    def __init__(self, name="lock"):
        self._name = name
        self._internal = threading.Lock()
        self._owner = None
        self._owner_base_priority = None
        self._owner_effective_priority = None
        self._waiters: OrderedDict[str, WaiterEntry] = OrderedDict()
        self._held_locks_by_task: dict = {}

    @property
    def name(self):
        return self._name

    @property
    def owner(self):
        return self._owner

    @property
    def owner_effective_priority(self):
        return self._owner_effective_priority

    def acquire(self, task_id, priority, timeout=None):
        with self._internal:
            if self._owner == task_id:
                return ACQUIRE_OK

            if self._owner is None:
                self._owner = task_id
                self._owner_base_priority = priority
                self._owner_effective_priority = priority
                self._register_hold(task_id)
                return ACQUIRE_OK

            if task_id in self._waiters:
                return self._waiters[task_id].result or ACQUIRE_TIMEOUT

            entry = WaiterEntry(task_id, priority)
            self._waiters[task_id] = entry
            self._reorder_waiters()

            if priority > self._owner_effective_priority:
                self._boost_owner(priority)

        ok = entry.event.wait(timeout=timeout) if timeout is not None else entry.event.wait()

        with self._internal:
            if entry.result is not None:
                return entry.result

            if not ok:
                if task_id in self._waiters:
                    del self._waiters[task_id]
                    self._reapply_inheritance()
                entry.result = ACQUIRE_TIMEOUT
                return ACQUIRE_TIMEOUT

            if self._owner == task_id:
                entry.result = ACQUIRE_OK
                return ACQUIRE_OK

            if task_id in self._waiters:
                del self._waiters[task_id]
                self._reapply_inheritance()
            entry.result = ACQUIRE_CANCELLED
            return ACQUIRE_CANCELLED

    def release(self, task_id):
        with self._internal:
            if self._owner != task_id:
                raise RuntimeError(
                    f"任务 {task_id} 不是锁 {self._name} 的持有者"
                    f"（当前持有者: {self._owner}）"
                )

            self._unregister_hold(task_id)

            if self._waiters:
                next_id, next_entry = next(iter(self._waiters.items()))
                del self._waiters[next_id]

                self._owner = next_id
                self._owner_base_priority = next_entry.priority
                self._owner_effective_priority = next_entry.priority
                self._register_hold(next_id)

                if self._waiters:
                    highest = max(w.priority for w in self._waiters.values())
                    if highest > self._owner_effective_priority:
                        self._boost_owner(highest)

                next_entry.result = ACQUIRE_OK
                next_entry.event.set()
            else:
                self._owner = None
                self._owner_base_priority = None
                self._owner_effective_priority = None

    def cancel(self, task_id):
        with self._internal:
            if task_id not in self._waiters:
                return False
            entry = self._waiters.pop(task_id)
            entry.result = ACQUIRE_CANCELLED
            entry.event.set()
            self._reapply_inheritance()
            return True

    def _reorder_waiters(self):
        items = sorted(self._waiters.items(), key=lambda kv: -kv[1].priority)
        self._waiters = OrderedDict(items)

    def _boost_owner(self, new_priority):
        old = self._owner_effective_priority
        self._owner_effective_priority = new_priority
        print(f"  [继承] {self._name}: 任务 {self._owner} 优先级 {old} → {new_priority}")

    def _reapply_inheritance(self):
        if self._owner is not None and self._waiters:
            highest = max(w.priority for w in self._waiters.values())
            base = self._owner_base_priority
            target = max(base, highest)
            if target != self._owner_effective_priority:
                self._boost_owner(target)
        elif self._owner is not None:
            if self._owner_effective_priority != self._owner_base_priority:
                old = self._owner_effective_priority
                self._owner_effective_priority = self._owner_base_priority
                print(
                    f"  [继承] {self._name}: 任务 {self._owner}"
                    f" 优先级 {old} → {self._owner_base_priority}（恢复）"
                )

    def _register_hold(self, task_id):
        if task_id not in self._held_locks_by_task:
            self._held_locks_by_task[task_id] = []
        self._held_locks_by_task[task_id].append(self)

    def _unregister_hold(self, task_id):
        if task_id in self._held_locks_by_task:
            locks = self._held_locks_by_task[task_id]
            if self in locks:
                locks.remove(self)
            if not locks:
                del self._held_locks_by_task[task_id]

    def get_held_locks(self, task_id):
        return list(self._held_locks_by_task.get(task_id, []))
