import threading
from collections import OrderedDict


ACQUIRE_OK = "acquired"
ACQUIRE_TIMEOUT = "timeout"
ACQUIRE_CANCELLED = "cancelled"
ACQUIRE_BLOCKED = "blocked"


class CeilingWaiterEntry:
    __slots__ = ("task_id", "priority", "event", "result")

    def __init__(self, task_id, priority):
        self.task_id = task_id
        self.priority = priority
        self.event = threading.Event()
        self.result = None


class CeilingLock:
    def __init__(self, lock_id, ceiling_priority):
        self.lock_id = lock_id
        self.ceiling_priority = ceiling_priority
        self._internal = threading.Lock()
        self._owner = None
        self._owner_base_priority = None
        self._owner_effective_priority = None
        self._waiters: OrderedDict[str, CeilingWaiterEntry] = OrderedDict()
        self._protocol = None

    @property
    def owner(self):
        return self._owner

    @property
    def owner_effective_priority(self):
        return self._owner_effective_priority

    def _set_protocol(self, protocol):
        self._protocol = protocol

    def acquire(self, task_id, priority, timeout=None):
        with self._internal:
            if self._owner == task_id:
                return ACQUIRE_OK

            if self._owner is None:
                if self._protocol and not self._protocol.can_acquire(task_id, priority, self.lock_id):
                    print(f"  [天花板] 拒绝: 任务 {task_id}(优先级{priority})"
                          f" 不能获取 {self.lock_id}（系统天花板规则）")
                    return ACQUIRE_BLOCKED

                self._owner = task_id
                self._owner_base_priority = priority
                self._owner_effective_priority = self.ceiling_priority
                if self._protocol:
                    self._protocol._notify_acquired(task_id, self)
                print(f"  [天花板] {self.lock_id}: 任务 {task_id}"
                      f" 获取锁，优先级 {priority} → {self.ceiling_priority}")
                return ACQUIRE_OK

            if task_id in self._waiters:
                return self._waiters[task_id].result or ACQUIRE_TIMEOUT

            entry = CeilingWaiterEntry(task_id, priority)
            self._waiters[task_id] = entry
            self._reorder_waiters()
            print(f"  [天花板] {self.lock_id}: 任务 {task_id}(优先级{priority}) 等待锁")

        ok = entry.event.wait(timeout=timeout) if timeout is not None else entry.event.wait()

        with self._internal:
            if entry.result is not None:
                return entry.result

            if not ok:
                if task_id in self._waiters:
                    del self._waiters[task_id]
                entry.result = ACQUIRE_TIMEOUT
                return ACQUIRE_TIMEOUT

            if self._owner == task_id:
                entry.result = ACQUIRE_OK
                return ACQUIRE_OK

            if task_id in self._waiters:
                del self._waiters[task_id]
            entry.result = ACQUIRE_CANCELLED
            return ACQUIRE_CANCELLED

    def release(self, task_id):
        with self._internal:
            if self._owner != task_id:
                raise RuntimeError(
                    f"任务 {task_id} 不是锁 {self.lock_id} 的持有者"
                    f"（当前持有者: {self._owner}）"
                )

            if self._protocol:
                self._protocol._notify_released(task_id, self)
            print(f"  [天花板] {self.lock_id}: 任务 {task_id}"
                  f" 释放锁，优先级 {self.ceiling_priority} → {self._owner_base_priority}")

            if self._waiters:
                next_id, next_entry = next(iter(self._waiters.items()))
                del self._waiters[next_id]

                can_proceed = True
                if self._protocol:
                    can_proceed = self._protocol.can_acquire(
                        next_id, next_entry.priority, self.lock_id
                    )

                if can_proceed:
                    self._owner = next_id
                    self._owner_base_priority = next_entry.priority
                    self._owner_effective_priority = self.ceiling_priority
                    if self._protocol:
                        self._protocol._notify_acquired(next_id, self)
                    next_entry.result = ACQUIRE_OK
                    next_entry.event.set()
                else:
                    self._owner = None
                    self._owner_base_priority = None
                    self._owner_effective_priority = None
                    next_entry.result = ACQUIRE_BLOCKED
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
            return True

    def _reorder_waiters(self):
        items = sorted(self._waiters.items(), key=lambda kv: -kv[1].priority)
        self._waiters = OrderedDict(items)


class PriorityCeilingProtocol:
    def __init__(self):
        self._locks: dict[str, CeilingLock] = {}
        self._held_by_task: dict[str, list[CeilingLock]] = {}
        self._internal = threading.Lock()

    def register_lock(self, lock_id, ceiling_priority):
        lock = CeilingLock(lock_id, ceiling_priority)
        lock._set_protocol(self)
        self._locks[lock_id] = lock
        return lock

    def get_lock(self, lock_id):
        return self._locks.get(lock_id)

    def system_ceiling(self):
        occupied = [lk for lk in self._locks.values() if lk.owner is not None]
        if not occupied:
            return 0
        return min(lk.ceiling_priority for lk in occupied)

    def can_acquire(self, task_id, task_priority, lock_id):
        lock = self._locks.get(lock_id)
        if lock is None:
            return False

        if task_priority > lock.ceiling_priority:
            return False

        for lk in self._locks.values():
            if lk.owner is not None and lk.owner != task_id:
                if task_priority <= lk.ceiling_priority:
                    return False

        return True

    def _notify_acquired(self, task_id, lock):
        if task_id not in self._held_by_task:
            self._held_by_task[task_id] = []
        self._held_by_task[task_id].append(lock)

    def _notify_released(self, task_id, lock):
        if task_id in self._held_by_task:
            locks = self._held_by_task[task_id]
            if lock in locks:
                locks.remove(lock)
            if not locks:
                del self._held_by_task[task_id]

    def get_held_locks(self, task_id):
        return list(self._held_by_task.get(task_id, []))

    def debug_state(self):
        lines = []
        lines.append(f"  系统天花板: {self.system_ceiling()}")
        for lid, lk in self._locks.items():
            owner_str = lk.owner if lk.owner else "空闲"
            lines.append(f"  锁 {lid}: 天花板={lk.ceiling_priority}, 持有者={owner_str}")
        return "\n".join(lines)
