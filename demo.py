import time
import threading
from priority_inheritance_lock import PriorityInheritanceLock
from priority_ceiling_lock import PriorityCeilingLock


class SimpleLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._owner = None

    def acquire(self, task_id, priority):
        with self._lock:
            if self._owner is None:
                self._owner = task_id
                return True
            return False

    def release(self, task_id):
        with self._lock:
            if self._owner == task_id:
                self._owner = None

    @property
    def owner(self):
        return self._owner


class TaskSimulator:
    def __init__(self):
        self._current_time = 0
        self._event_log = []

    def log(self, message):
        self._event_log.append(message)
        print(f"  [t={self._current_time}] {message}")

    def advance_time(self, units):
        self._current_time += units

    def get_log(self):
        return self._event_log


def simulate_priority_inversion():
    print("=" * 70)
    print("场景一：无保护的优先级反转问题")
    print("=" * 70)

    sim = TaskSimulator()
    lock = SimpleLock()

    print("\n初始状态：")
    print("  低优先级任务 L (优先级 1)")
    print("  中优先级任务 M (优先级 5)")
    print("  高优先级任务 H (优先级 10)")
    print("  共享资源锁 S")

    print("\n--- 时间线 ---")

    sim.log("任务 L 开始运行")
    sim.advance_time(1)

    sim.log("任务 L 获取锁 S")
    lock.acquire("L", 1)

    sim.advance_time(2)
    sim.log("任务 L 持有锁，进行临界区操作...")

    sim.advance_time(1)
    sim.log("任务 H 就绪，优先级 10 > 1，抢占 L")

    sim.advance_time(1)
    sim.log("任务 H 尝试获取锁 S... 失败，被阻塞")

    sim.advance_time(1)
    sim.log("任务 H 等待锁 S，切换回任务 L 继续执行")

    sim.advance_time(1)
    sim.log("任务 M 就绪，优先级 5 > 1，抢占 L")

    sim.log("⚠️  优先级反转发生！")
    sim.log("   高优先级 H 等待锁，但持锁的 L 被 M 抢占")
    sim.log("   H 实际上被优先级更低的 M 间接阻塞")

    for i in range(5):
        sim.advance_time(1)
        sim.log(f"任务 M 长时间运行中... ({i+1}/5)")

    sim.advance_time(1)
    sim.log("任务 M 完成，L 恢复执行")

    sim.advance_time(2)
    sim.log("任务 L 终于释放锁 S")
    lock.release("L")

    sim.advance_time(1)
    sim.log("任务 H 获取锁 S，继续执行")

    print(f"\n统计：高优先级任务 H 被阻塞了约 {sim._current_time - 6} 个时间单位")
    print("     其中大部分时间是被中优先级任务 M 间接阻塞的")


def simulate_priority_inheritance():
    print("\n" + "=" * 70)
    print("场景二：优先级继承协议 (Priority Inheritance)")
    print("=" * 70)

    sim = TaskSimulator()
    lock = PriorityInheritanceLock()

    print("\n核心原理：当高优先级任务等待锁时，")
    print("         持锁的低优先级任务临时继承高优先级任务的优先级")

    print("\n--- 时间线 ---")

    sim.log("任务 L 开始运行 (优先级 1)")
    sim.advance_time(1)

    sim.log("任务 L 获取锁 S")
    lock.acquire("L", 1)
    sim.log(f"  锁持有者: L, 有效优先级: {lock.owner_effective_priority}")

    sim.advance_time(2)
    sim.log("任务 L 持有锁，进行临界区操作...")

    sim.advance_time(1)
    sim.log("任务 H 就绪 (优先级 10)，尝试抢占...")

    sim.log("任务 H 尝试获取锁 S... 失败，进入等待队列")
    lock.acquire("H", 10)
    sim.log(f"  锁持有者: L, 有效优先级: {lock.owner_effective_priority}")

    sim.log("✅ 优先级继承生效：L 的优先级提升到 10")
    sim.log("   现在 L 不会被中优先级任务抢占了")

    sim.advance_time(1)
    sim.log("任务 M 就绪 (优先级 5)...")
    sim.log("   但 L 当前有效优先级为 10 > 5，M 无法抢占！")
    sim.log("   M 只能等待，优先级反转被避免")

    for i in range(3):
        sim.advance_time(1)
        sim.log(f"任务 L 继续执行临界区... ({i+1}/3)")

    sim.advance_time(1)
    sim.log("任务 L 释放锁 S，优先级恢复为 1")
    lock.release("L")

    sim.advance_time(1)
    sim.log("任务 H 获取锁 S，立即执行")

    sim.advance_time(2)
    sim.log("任务 H 完成并释放锁")
    lock.release("H")

    sim.advance_time(1)
    sim.log("任务 M 终于可以运行了")

    print(f"\n统计：高优先级任务 H 仅被阻塞了约 {sim._current_time - 6} 个时间单位")
    print("     远短于无保护场景，中优先级任务无法插队")


def simulate_priority_ceiling():
    print("\n" + "=" * 70)
    print("场景三：优先级天花板协议 (Priority Ceiling)")
    print("=" * 70)

    sim = TaskSimulator()

    ceiling_priority = 10
    lock = PriorityCeilingLock(ceiling_priority)

    print(f"\n核心原理：每个锁预先设定优先级天花板（此处设为 {ceiling_priority}），")
    print("         任务获取锁时，其优先级立即提升到天花板值")

    print("\n--- 时间线 ---")

    sim.log("任务 L 开始运行 (优先级 1)")
    sim.advance_time(1)

    sim.log("任务 L 获取锁 S")
    lock.acquire("L", 1)
    sim.log(f"  锁天花板: {lock.ceiling_priority}")
    sim.log(f"  持有者 L 的有效优先级提升到: {lock.owner_effective_priority}")

    sim.log("✅ 获取锁时立即提升优先级，从根源上防止抢占")

    sim.advance_time(2)
    sim.log("任务 L 持有锁，进行临界区操作...")

    sim.advance_time(1)
    sim.log("任务 M 就绪 (优先级 5)...")
    sim.log("   L 当前优先级为 10 > 5，M 无法抢占")

    sim.advance_time(1)
    sim.log("任务 H 就绪 (优先级 10)...")
    sim.log("   H 尝试获取锁 S，但 L 已持有")
    acquired = lock.acquire("H", 10)
    sim.log(f"   获取结果: {acquired}，H 等待锁释放")

    sim.log("   注意：H 优先级等于天花板，不违反协议")

    for i in range(3):
        sim.advance_time(1)
        sim.log(f"任务 L 继续执行临界区... ({i+1}/3)")

    sim.advance_time(1)
    sim.log("任务 L 释放锁 S，优先级恢复为 1")
    lock.release("L")

    sim.advance_time(1)
    sim.log("任务 H 获取锁 S，优先级提升到 10")
    lock.acquire("H", 10)

    sim.advance_time(2)
    sim.log("任务 H 完成并释放锁")
    lock.release("H")

    sim.advance_time(1)
    sim.log("任务 M 终于可以运行了")

    print(f"\n统计：高优先级任务 H 被阻塞了约 {sim._current_time - 7} 个时间单位")
    print("     阻塞时间可预测，等于临界区最长执行时间")


def compare_solutions():
    print("\n" + "=" * 70)
    print("方案对比总结")
    print("=" * 70)

    print("\n1. 优先级继承协议 (Priority Inheritance)")
    print("   ✓ 动态调整，适应性强")
    print("   ✓ 仅在有高优先级任务等待时才提升")
    print("   ✗ 实现较复杂，需追踪所有等待者")
    print("   ✗ 可能出现多次优先级调整")
    print("   ✗ 不能防止死锁")
    print("   适用：任务优先级动态变化，或锁使用模式不确定的系统")

    print("\n2. 优先级天花板协议 (Priority Ceiling)")
    print("   ✓ 实现简单，静态配置天花板值")
    print("   ✓ 阻塞时间可预测（等于最长临界区）")
    print("   ✓ 可防止死锁（按规则使用时）")
    print("   ✗ 可能不必要地提升优先级（无高优任务时）")
    print("   ✗ 需预先知道所有任务和锁的关系")
    print("   适用：系统设计时已知任务优先级和锁使用场景")

    print("\n3. 共同点")
    print("   - 都通过提升持锁任务优先级来防止中优先级任务抢占")
    print("   - 都能有效解决优先级反转问题")
    print("   - 都需要操作系统/运行时的支持")


if __name__ == "__main__":
    simulate_priority_inversion()
    print("\n")
    time.sleep(0.5)

    simulate_priority_inheritance()
    print("\n")
    time.sleep(0.5)

    simulate_priority_ceiling()
    print("\n")
    time.sleep(0.5)

    compare_solutions()
