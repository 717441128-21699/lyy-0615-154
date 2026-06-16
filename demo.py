import time
import threading
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from priority_inheritance_lock import PriorityInheritanceLock, ACQUIRE_OK, ACQUIRE_TIMEOUT, ACQUIRE_CANCELLED
from priority_ceiling_lock import PriorityCeilingProtocol, ACQUIRE_BLOCKED


def _result_str(r):
    mapping = {
        ACQUIRE_OK: "获取锁",
        ACQUIRE_TIMEOUT: "超时",
        ACQUIRE_CANCELLED: "被取消",
        ACQUIRE_BLOCKED: "被拒绝(天花板规则)",
    }
    return mapping.get(r, r)


def demo_priority_inversion():
    print("=" * 70)
    print("场景一：优先级反转问题（无保护）")
    print("=" * 70)
    print("  任务 L(优先级1) 持有锁S → H(优先级10) 等待锁 → M(优先级5) 抢占 L")
    print("  结果：高优先级 H 被低优先级 M 间接阻塞\n")

    print("  [t=0] L 获取锁 S")
    print("  [t=2] H 尝试获取锁 S → 阻塞等待")
    print("  [t=3] M 就绪，抢占 L（因 M优先级5 > L优先级1）")
    print("  [t=3~8] M 长时间运行，L 无法释放锁，H 一直等")
    print("  [t=8] M 完成，L 恢复执行")
    print("  [t=10] L 释放锁，H 终于获取")
    print("\n  ⚠️  H 被阻塞约 8 个时间单位，大部分是被 M 间接阻塞")


def demo_inheritance_basic():
    print("\n" + "=" * 70)
    print("场景二：优先级继承 — 基本流程（阻塞等待 + 超时 + 取消）")
    print("=" * 70)

    lock = PriorityInheritanceLock("S")

    print("\n--- 1. 基本阻塞等待 ---")
    r = lock.acquire("L", 1)
    print(f"  L(优先级1) 获取锁 S: {_result_str(r)}")
    print(f"  锁持有者: {lock.owner}, 有效优先级: {lock.owner_effective_priority}")

    def h_wait():
        r = lock.acquire("H", 10)
        print(f"  H(优先级10) 等待结果: {_result_str(r)}")

    t_h = threading.Thread(target=h_wait)
    t_h.start()
    time.sleep(0.1)

    print(f"  锁持有者: {lock.owner}, 有效优先级: {lock.owner_effective_priority}")
    print("  ✅ L 继承了 H 的优先级10，M 无法抢占")

    lock.release("L")
    print(f"  L 释放锁，当前持有者: {lock.owner}, 有效优先级: {lock.owner_effective_priority}")
    t_h.join(timeout=2)

    print("\n--- 2. 超时等待 ---")
    lock2 = PriorityInheritanceLock("S2")
    lock2.acquire("L", 1)
    print(f"  L 获取锁 S2")

    r = lock2.acquire("H", 10, timeout=0.3)
    print(f"  H 等待锁 S2 (0.3秒超时): {_result_str(r)}")

    lock2.release("L")
    print(f"  L 释放锁 S2")

    print("\n--- 3. 取消等待 ---")
    lock3 = PriorityInheritanceLock("S3")
    lock3.acquire("L", 1)
    print(f"  L 获取锁 S3")

    cancelled_result = [None]

    def m_wait():
        cancelled_result[0] = lock3.acquire("M", 5)

    t_m = threading.Thread(target=m_wait)
    t_m.start()
    time.sleep(0.1)

    ok = lock3.cancel("M")
    print(f"  取消 M 的等待: {ok}")
    t_m.join(timeout=2)
    print(f"  M 的等待结果: {_result_str(cancelled_result[0])}")

    lock3.release("L")
    print(f"  L 释放锁 S3")


def demo_nested_inheritance():
    print("\n" + "=" * 70)
    print("场景三：嵌套锁 + 多级优先级继承")
    print("=" * 70)
    print("  任务 A(优先级1) 持有锁S1，等待锁S2")
    print("  任务 B(优先级5) 持有锁S2")
    print("  任务 C(优先级10) 等待锁S1")
    print("  传递链: C等S1 → A持S1等S2 → B持S2")
    print("  期望: C的优先级通过S1传给A，再通过S2传给B\n")

    lock_s1 = PriorityInheritanceLock("S1")
    lock_s2 = PriorityInheritanceLock("S2")

    lock_s1.acquire("A", 1)
    print(f"  A(优先级1) 获取锁 S1")

    lock_s2.acquire("B", 5)
    print(f"  B(优先级5) 获取锁 S2")

    def a_waits_s2():
        r = lock_s2.acquire("A", 1)
        print(f"  A 获取锁 S2: {_result_str(r)} (A此时有效优先级应被S2继承)")
        time.sleep(0.1)
        lock_s2.release("A")
        print(f"  A 释放锁 S2")
        time.sleep(0.1)
        lock_s1.release("A")
        print(f"  A 释放锁 S1")

    t_a = threading.Thread(target=a_waits_s2)
    t_a.start()
    time.sleep(0.1)

    print(f"  A 等待 S2, S2 持有者: {lock_s2.owner}, 有效优先级: {lock_s2.owner_effective_priority}")

    def c_waits_s1():
        r = lock_s1.acquire("C", 10)
        print(f"  C(优先级10) 获取锁 S1: {_result_str(r)}")
        time.sleep(0.05)
        lock_s1.release("C")
        print(f"  C 释放锁 S1")

    t_c = threading.Thread(target=c_waits_s1)
    t_c.start()
    time.sleep(0.2)

    print(f"\n  当前状态:")
    print(f"    S1 持有者: {lock_s1.owner}, 有效优先级: {lock_s1.owner_effective_priority}")
    print(f"    S2 持有者: {lock_s2.owner}, 有效优先级: {lock_s2.owner_effective_priority}")

    lock_s2.release("B")
    print(f"  B 释放锁 S2")

    t_a.join(timeout=3)
    t_c.join(timeout=3)

    print("\n  ✅ 多级继承: C(10) → S1提升A → A等S2 → S2提升B → B以高优先级完成")


def demo_ceiling_multi_lock():
    print("\n" + "=" * 70)
    print("场景四：多锁天花板协议 — 系统天花板动态判断")
    print("=" * 70)
    print("  锁 A: 天花板=5 (低优先级资源)")
    print("  锁 B: 天花板=10 (高优先级资源)")
    print("  规则: 当任何锁被占用时，系统天花板=已占用锁的天花板最小值")
    print("        新任务只能在其优先级严格大于系统天花板时获取锁\n")

    protocol = PriorityCeilingProtocol()
    lock_a = protocol.register_lock("A", 5)
    lock_b = protocol.register_lock("B", 10)

    print("--- 初始状态 ---")
    print(protocol.debug_state())

    print("\n--- 步骤1: 低优先级任务 L(优先级1) 获取锁 A ---")
    r = lock_a.acquire("L", 1)
    print(f"  结果: {_result_str(r)}")
    print(protocol.debug_state())

    print("\n--- 步骤2: 中优先级任务 M(优先级3) 尝试获取锁 B ---")
    r = lock_b.acquire("M", 3)
    print(f"  结果: {_result_str(r)}")
    print(f"  原因: M优先级3 <= 系统天花板5(锁A被占用) → 被拒绝")

    print("\n--- 步骤3: 高优先级任务 H(优先级8) 尝试获取锁 B ---")
    r = lock_b.acquire("H", 8)
    print(f"  结果: {_result_str(r)}")
    print(f"  原因: H优先级8 > 系统天花板5(锁A被占用) → 允许")

    print("\n--- 步骤4: L 释放锁 A ---")
    lock_a.release("L")
    print(protocol.debug_state())

    print("\n--- 步骤5: H 释放锁 B, M 再次尝试 ---")
    lock_b.release("H")
    r = lock_b.acquire("M", 3)
    print(f"  M 获取锁 B: {_result_str(r)}")
    print(f"  原因: 无锁被占用, 系统天花板=0 → M可以获取")

    lock_b.release("M")
    print("\n  ✅ 天花板协议成功阻止了不符合规则的获取，避免了优先级反转")


def demo_ceiling_vs_inheritance():
    print("\n" + "=" * 70)
    print("场景五：复杂场景对比 — 嵌套锁下的优先级反转防护")
    print("=" * 70)

    print("\n>>> 优先级继承方案 <<<\n")
    pi_s1 = PriorityInheritanceLock("S1")
    pi_s2 = PriorityInheritanceLock("S2")

    pi_s1.acquire("L", 1)
    pi_s2.acquire("M", 5)
    print(f"  L(1) 持有S1, M(5) 持有S2")

    def pi_l_waits_s2():
        r = pi_s2.acquire("L", 1)
        print(f"  L 等待 S2: {_result_str(r)}")
        pi_s2.release("L")
        pi_s1.release("L")

    def pi_h_waits_s1():
        r = pi_s1.acquire("H", 10)
        print(f"  H 等待 S1: {_result_str(r)}")
        pi_s1.release("H")

    t1 = threading.Thread(target=pi_l_waits_s2)
    t2 = threading.Thread(target=pi_h_waits_s1)
    t1.start()
    time.sleep(0.1)
    t2.start()
    time.sleep(0.2)

    print(f"  S1: 持有者={pi_s1.owner}, 有效优先级={pi_s1.owner_effective_priority}")
    print(f"  S2: 持有者={pi_s2.owner}, 有效优先级={pi_s2.owner_effective_priority}")

    pi_s2.release("M")
    t1.join(timeout=3)
    t2.join(timeout=3)

    print("\n>>> 天花板方案 <<<\n")
    protocol = PriorityCeilingProtocol()
    ce_s1 = protocol.register_lock("S1", 10)
    ce_s2 = protocol.register_lock("S2", 10)

    r1 = ce_s1.acquire("L", 1)
    print(f"  L(1) 获取 S1: {_result_str(r1)} (优先级提升到10)")
    print(f"  系统天花板: {protocol.system_ceiling()}")

    r2 = ce_s2.acquire("M", 5)
    print(f"  M(5) 尝试获取 S2: {_result_str(r2)}")
    print(f"  原因: M优先级5 <= 系统天花板10(S1被占用) → 被拒绝")
    print("  ✅ 天花板方案从源头阻止了M获取锁，不会产生反转")

    ce_s1.release("L")
    r3 = ce_s2.acquire("M", 5)
    print(f"  L 释放S1后, M 再次获取 S2: {_result_str(r3)}")
    ce_s2.release("M")


def demo_comparison():
    print("\n" + "=" * 70)
    print("方案对比总结")
    print("=" * 70)

    table = """
  ┌──────────────────┬──────────────────────┬──────────────────────┐
  │       维度        │    优先级继承(PI)     │   优先级天花板(PC)    │
  ├──────────────────┼──────────────────────┼──────────────────────┤
  │ 提升时机         │ 高优先级等待时才提升   │ 获取锁时立即提升      │
  │ 提升程度         │ 提升到等待者优先级     │ 提升到预设天花板      │
  │ 多锁场景         │ 需沿等待链逐级传递     │ 系统天花板一次判定    │
  │ 阻塞可预测性     │ 较弱(动态调整)        │ 强(=最长临界区)       │
  │ 死锁防护         │ 不提供                │ 协议本身可防止        │
  │ 优先级浪费       │ 少(按需提升)          │ 可能多余(无高优等待)  │
  │ 实现复杂度       │ 高(追踪等待链)        │ 低(静态配置+规则)     │
  │ 适用前提         │ 优先级动态、模式未知   │ 已知任务优先级和锁关系 │
  │ 典型应用         │ Linux futex, POSIX    │ VxWorks, AUTOSAR     │
  └──────────────────┴──────────────────────┴──────────────────────┘
    """
    print(table)

    print("  核心差异:")
    print("  • PI 是\"被动救火\"——反转发生后，通过继承减少损失")
    print("  • PC 是\"主动防御\"——反转发生前，通过规则杜绝可能")
    print("  • 复杂场景下，PI 的多级传递链条可能很长，PC 的系统天花板一步到位")
    print("  • PI 更灵活，PC 更确定；选择取决于系统对可预测性的要求")


if __name__ == "__main__":
    demo_priority_inversion()
    demo_inheritance_basic()
    demo_nested_inheritance()
    demo_ceiling_multi_lock()
    demo_ceiling_vs_inheritance()
    demo_comparison()
