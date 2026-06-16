import threading
from collections import OrderedDict


ACQUIRE_OK = "acquired"
ACQUIRE_TIMEOUT = "timeout"
ACQUIRE_CANCELLED = "cancelled"
ALREADY_WAITING = "already_waiting"


class WaiterEntry:
    __slots__ = ("task_id", "priority", "event", "result")

    def __init__(self, task_id, priority):
        self.task_id = task_id
        self.priority = priority
        self.event = threading.Event()
        self.result = None


class InheritanceGraph:
    def __init__(self):
        self._waiting_for = {}
        self._locks = []

    def register_lock(self, lock):
        if lock not in self._locks:
            self._locks.append(lock)

    def set_waiting_for(self, task_id, lock):
        self._waiting_for[task_id] = lock

    def clear_waiting_for(self, task_id):
        if task_id in self._waiting_for:
            del self._waiting_for[task_id]

    def get_waiting_for(self, task_id):
        return self._waiting_for.get(task_id)

    def trace_chain(self, start_lock):
        chain = []
        visited_locks = set()
        current_lock = start_lock

        while current_lock is not None and current_lock not in visited_locks:
            visited_locks.add(current_lock)
            owner = current_lock.owner
            if owner is None:
                chain.append((current_lock, None, None))
                break
            next_lock = self._waiting_for.get(owner)
            chain.append((current_lock, owner, next_lock))
            current_lock = next_lock

        return chain

    def boost_chain(self, start_lock, target_priority, source_task=None):
        chain = self.trace_chain(start_lock)
        promoted = []

        for lock, owner, next_lock in chain:
            if owner is None:
                break
            if (lock._owner_effective_priority or 0) < target_priority:
                old = lock._owner_effective_priority
                lock._owner_effective_priority = target_priority
                promoted.append((lock, owner, old, target_priority))
                if source_task and source_task != owner:
                    print(
                        f"  [链传] {lock.name}: 任务 {owner} 优先级 {old} → {target_priority}"
                        f" (沿等待链来自 {source_task})"
                    )
                else:
                    print(
                        f"  [继承] {lock.name}: 任务 {owner} 优先级 {old} → {target_priority}"
                    )

        return promoted

    def reapply_all(self):
        for lock in self._locks:
            if lock.owner is None:
                continue
            base = lock._owner_base_priority
            if lock._owner_effective_priority != base:
                old = lock._owner_effective_priority
                lock._owner_effective_priority = base
                print(
                    f"  [继承] {lock.name}: 任务 {lock.owner}"
                    f" 优先级 {old} → {base}（恢复）"
                )

        locks_with_waiters = []
        for lock in self._locks:
            if lock.owner is not None and lock._waiters:
                highest = max(w.priority for w in lock._waiters.values())
                locks_with_waiters.append((lock, highest))

        locks_with_waiters.sort(key=lambda x: -x[1])

        for lock, highest in locks_with_waiters:
            self.boost_chain(lock, highest)

    def print_chain(self, start_lock, indent="  "):
        chain = self.trace_chain(start_lock)
        parts = []
        for i, (lock, owner, next_lock) in enumerate(chain):
            if owner:
                eff_pri = lock._owner_effective_priority
                base_pri = lock._owner_base_priority
                pri_str = f"{base_pri}→{eff_pri}" if eff_pri != base_pri else f"{eff_pri}"
                parts.append(f"{lock.name}[{owner}({pri_str})]")
                if next_lock:
                    parts.append("→")
            else:
                parts.append(f"{lock.name}[free]")
        print(f"{indent}等待链: {' '.join(parts)}")


_default_graph = InheritanceGraph()


class PriorityInheritanceLock:
    def __init__(self, name="lock", graph=None):
        self._name = name
        self._internal = threading.Lock()
        self._owner = None
        self._owner_base_priority = None
        self._owner_effective_priority = None
        self._waiters: OrderedDict[str, WaiterEntry] = OrderedDict()
        self._held_locks_by_task: dict = {}
        self._graph = graph or _default_graph
        self._graph.register_lock(self)

    @property
    def name(self):
        return self._name

    @property
    def owner(self):
        return self._owner

    @property
    def owner_base_priority(self):
        return self._owner_base_priority

    @property
    def owner_effective_priority(self):
        return self._owner_effective_priority

    @property
    def waiters(self):
        return list(self._waiters.keys())

    def get_held_locks(self, task_id):
        return list(self._held_locks_by_task.get(task_id, []))

    def print_chain(self, indent="  "):
        self._graph.print_chain(self, indent)

    def acquire(self, task_id, priority, timeout=None):
        with self._internal:
            if self._owner == task_id:
                return ACQUIRE_OK

            if self._owner is None:
                self._owner = task_id
                self._owner_base_priority = priority
                self._owner_effective_priority = priority
                self._register_hold(task_id)
                self._graph.clear_waiting_for(task_id)
                return ACQUIRE_OK

            if task_id in self._waiters:
                return ALREADY_WAITING

            entry = WaiterEntry(task_id, priority)
            self._waiters[task_id] = entry
            self._reorder_waiters()
            self._graph.set_waiting_for(task_id, self)

            if priority > (self._owner_effective_priority or 0):
                self._graph.boost_chain(self, priority, source_task=task_id)

            self._graph.reapply_all()

            if timeout is not None and timeout < 0:
                return "waiting"

        ok = entry.event.wait(timeout=timeout) if timeout is not None else entry.event.wait()

        with self._internal:
            if entry.result is not None:
                return entry.result

            if not ok:
                if task_id in self._waiters:
                    del self._waiters[task_id]
                    self._graph.clear_waiting_for(task_id)
                    self._graph.reapply_all()
                entry.result = ACQUIRE_TIMEOUT
                return ACQUIRE_TIMEOUT

            if self._owner == task_id:
                self._graph.clear_waiting_for(task_id)
                entry.result = ACQUIRE_OK
                return ACQUIRE_OK

            if task_id in self._waiters:
                del self._waiters[task_id]
                self._graph.clear_waiting_for(task_id)
                self._graph.reapply_all()
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
                self._graph.clear_waiting_for(next_id)

                if self._waiters:
                    highest = max(w.priority for w in self._waiters.values())
                    if highest > self._owner_effective_priority:
                        self._graph.boost_chain(self, highest)

                next_entry.result = ACQUIRE_OK
                next_entry.event.set()
            else:
                self._owner = None
                self._owner_base_priority = None
                self._owner_effective_priority = None

            self._graph.reapply_all()

    def cancel(self, task_id):
        with self._internal:
            if task_id not in self._waiters:
                return False
            entry = self._waiters.pop(task_id)
            entry.result = ACQUIRE_CANCELLED
            entry.event.set()
            self._graph.clear_waiting_for(task_id)
            self._graph.reapply_all()
            return True

    def _reorder_waiters(self):
        items = sorted(self._waiters.items(), key=lambda kv: -kv[1].priority)
        self._waiters = OrderedDict(items)

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
