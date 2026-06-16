import threading


class PriorityCeilingLock:
    def __init__(self, ceiling_priority):
        self._lock = threading.Lock()
        self._ceiling_priority = ceiling_priority
        self._owner = None
        self._owner_original_priority = None

    def acquire(self, task_id, task_priority):
        if task_priority > self._ceiling_priority:
            raise RuntimeError(
                f"Task {task_id} priority ({task_priority}) exceeds "
                f"lock ceiling priority ({self._ceiling_priority})"
            )

        with self._lock:
            if self._owner is None:
                self._owner = task_id
                self._owner_original_priority = task_priority
                print(f"  [天花板] 任务 {task_id} 获取锁，优先级提升到 {self._ceiling_priority}")
                return True
            return False

    def release(self, task_id):
        with self._lock:
            if self._owner != task_id:
                raise RuntimeError(f"Task {task_id} does not own the lock")

            print(f"  [天花板] 任务 {task_id} 释放锁，优先级恢复为 {self._owner_original_priority}")
            self._owner = None
            self._owner_original_priority = None

    @property
    def owner(self):
        return self._owner

    @property
    def ceiling_priority(self):
        return self._ceiling_priority

    @property
    def owner_effective_priority(self):
        if self._owner is not None:
            return self._ceiling_priority
        return None


class PriorityCeilingProtocol:
    def __init__(self):
        self._locks = {}
        self._task_priorities = {}

    def register_lock(self, lock_id, ceiling_priority):
        self._locks[lock_id] = PriorityCeilingLock(ceiling_priority)

    def get_lock(self, lock_id):
        return self._locks.get(lock_id)

    def compute_system_ceiling(self):
        if not self._locks:
            return 0
        return max(lock.ceiling_priority for lock in self._locks.values() if lock.owner is None)

    def can_acquire(self, task_id, task_priority, lock_id):
        lock = self._locks.get(lock_id)
        if lock is None:
            return False

        if task_priority > lock.ceiling_priority:
            return False

        for other_lock in self._locks.values():
            if other_lock.owner is not None and other_lock.owner != task_id:
                if task_priority <= other_lock.ceiling_priority:
                    return False
        return True
