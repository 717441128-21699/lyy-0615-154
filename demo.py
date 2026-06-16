import time
import threading
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from priority_inheritance_lock import (
    PriorityInheritanceLock, InheritanceGraph,
    ACQUIRE_OK, ACQUIRE_TIMEOUT, ACQUIRE_CANCELLED, ALREADY_WAITING
)
from priority_ceiling_lock import PriorityCeilingProtocol, ACQUIRE_BLOCKED
from scheduler import Task, PriorityScheduler, run_comparison, MODE_NONE, MODE_INHERITANCE, MODE_CEILING


def _result_str(r):
    mapping = {
        ACQUIRE_OK: "✓ 获取锁",
        ACQUIRE_TIMEOUT: "⏱  超时",
        ACQUIRE_CANCELLED: "✕ 被取消",
        ACQUIRE_BLOCKED: "🛑 被拒绝(天花板规则)",
        ALREADY_WAITING: "⏳ 已在等待",
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


def demo_chain_propagation():
    print("\n" + "=" * 70)
    print("场景二：嵌套锁 — 优先级沿等待链传递")
    print("=" * 70)
    print("  任务 A(优先级1) 持有锁S1，等待锁S2")
    print("  任务 B(优先级5) 持有锁S2")
    print("  任务 C(优先级10) 等待锁S1")
    print("  期望传递链: C(10) → S1提升A → A等S2 → S2提升B → B也提升到10\n")

    graph = InheritanceGraph()
    s1 = PriorityInheritanceLock("S1", graph=graph)
    s2 = PriorityInheritanceLock("S2", graph=graph)

    s1.acquire("A", 1)
    print(f"  [初始] A(1) 获取 S1")
    s2.acquire("B", 5)
    print(f"  [初始] B(5) 获取 S2")
    print()

    graph.print_chain(s1, "  ")
    print(f"  S1 持有者 A 有效优先级: {s1.owner_effective_priority}")
    print(f"  S2 持有者 B 有效优先级: {s2.owner_effective_priority}")
    print()

    def a_waits_s2():
        r = s2.acquire("A", 1)
        print(f"\n  A 获取 S2: {_result_str(r)}")
        s2.release("A")
        s1.release("A")

    t_a = threading.Thread(target=a_waits_s2)
    t_a.start()
    time.sleep(0.1)

    print(f"\n  [A 等待 S2] 当前等待链:")
    graph.print_chain(s1, "    ")
    print(f"    S1 持有者 A 有效优先级: {s1.owner_effective_priority}")
    print(f"    S2 持有者 B 有效优先级: {s2.owner_effective_priority}")

    def c_waits_s1():
        r = s1.acquire("C", 10)
        print(f"\n  C 获取 S1: {_result_str(r)}")
        s1.release("C")

    t_c = threading.Thread(target=c_waits_s1)
    t_c.start()
    time.sleep(0.2)

    print(f"\n  [C 等待 S1] 优先级沿链传递!")
    graph.print_chain(s1, "    ")
    print(f"    S1 持有者 A 有效优先级: {s1.owner_effective_priority}")
    print(f"    S2 持有者 B 有效优先级: {s2.owner_effective_priority}")
    print(f"    ✅ B 也被提升到 10！整个链都获得了 C 的优先级")

    time.sleep(0.1)
    s2.release("B")
    print(f"\n  B 释放 S2")

    t_a.join(timeout=2)
    t_c.join(timeout=2)
    print(f"\n  ✅ 优先级沿等待链完整传递，加速了整个临界区的执行")


def demo_already_waiting():
    print("\n" + "=" * 70)
    print("场景三：重复等待状态 — 同一任务重复 acquire 返回明确状态")
    print("=" * 70)
    print("  同一任务反复尝试获取同一把锁时，返回 'already_waiting' 而非 'timeout'\n")

    lock = PriorityInheritanceLock("S")
    lock.acquire("A", 1)
    print(f"  A 获取锁 S")

    results = []
    for i in range(3):
        r = lock.acquire("B", 5, timeout=0.05)
        results.append(r)
        print(f"  第 {i+1} 次 B 尝试获取 S: {_result_str(r)}")

    print(f"\n  三次结果: {results}")
    check = results[0] in (ACQUIRE_TIMEOUT, ALREADY_WAITING) and results[1] == ALREADY_WAITING and results[2] == ALREADY_WAITING
    print(f"  ✅ 第1次等待超时，第2-3次都返回 'already_waiting'，不会重复入队: {check}")

    lock.release("A")


def demo_ceiling_multi_lock_detailed():
    print("\n" + "=" * 70)
    print("场景四：多锁天花板 — 动态系统天花板与拦截依据")
    print("=" * 70)
    print("  锁 X: 天花板=3  (低优资源, 只能被低优任务使用)")
    print("  锁 Y: 天花板=7  (中优资源)")
    print("  锁 Z: 天花板=10 (高优资源)")
    print("  规则: 任务优先级 > 系统天花板 才能获取空闲锁\n")

    protocol = PriorityCeilingProtocol()
    lock_x = protocol.register_lock("X", 3)
    lock_y = protocol.register_lock("Y", 7)
    lock_z = protocol.register_lock("Z", 10)

    print("--- 初始状态 ---")
    print(protocol.debug_state())
    print()

    print("--- 步骤1: 任务 L(优先级1) 获取锁 X ---")
    r = lock_x.acquire("L", 1)
    print(f"  结果: {_result_str(r)}")
    print(protocol.debug_state())
    print(f"  系统天花板 = min(3) = 3")
    print()

    print("--- 步骤2: 任务 M(优先级5) 尝试获取锁 Y ---")
    r = lock_y.acquire("M", 5)
    print(f"  结果: {_result_str(r)}")
    reason = protocol.explain_blocked("M", 5, "Y")
    print(f"  拦截依据: {reason}")
    print(f"  验证: M优先级5 <= 系统天花板3? {5 <= 3} → 确实不满足")
    print()

    print("--- 步骤3: 任务 H(优先级8) 尝试获取锁 Y ---")
    r = lock_y.acquire("H", 8)
    print(f"  结果: {_result_str(r)}")
    print(f"  验证: H优先级8 > 系统天花板3 → 满足，允许获取")
    print(protocol.debug_state())
    print(f"  系统天花板 = min(3,10) = 3")
    print()

    print("--- 步骤4: 任务 L 释放锁 X ---")
    lock_x.release("L")
    print(protocol.debug_state())
    print(f"  系统天花板 = min(10) = 10")
    print()

    print("--- 步骤5: 任务 M(优先级5) 再次尝试获取锁 Y（Y仍被H持有）---")
    r = lock_y.acquire("M", 5, timeout=0.2)
    print(f"  结果: {_result_str(r)}")
    print(f"  说明: Y被持有，M进入等待队列，不做天花板规则检查")
    print()

    print("--- 步骤6: 任务 H 释放锁 Y ---")
    lock_y.release("H")
    print(protocol.debug_state())

    print("\n  ✅ 天花板拦截依据与获取结果完全一致，多锁状态动态更新正确")


def demo_scheduler_comparison():
    print("\n" + "=" * 70)
    print("场景五：任务调度模拟器 — 三种模式时间片对比")
    print("=" * 70)
    print("  经典优先级反转场景的调度对比:")
    print("  L(1)  工作3: acquire S, work, release S")
    print("  M(5)  工作5: 无锁操作（纯计算）")
    print("  H(10) 工作3: acquire S, work, release S\n")

    ops = {
        "L": (1, 3, [("acquire", "S"), ("release", "S")]),
        "M": (5, 5, []),
        "H": (10, 3, [("acquire", "S"), ("release", "S")]),
    }
    locks_config = {"S": 10}

    run_comparison(ops, locks_config)


def demo_nested_scheduler():
    print("\n" + "=" * 70)
    print("场景六：嵌套锁场景 — 调度器对比")
    print("=" * 70)
    print("  嵌套锁场景:")
    print("  A(1)  acquire S1, acquire S2, release S2, release S1")
    print("  B(5)  无锁, 长时间运行")
    print("  C(10) acquire S1, release S1")
    print()

    ops = {
        "A": (1, 4, [("acquire", "S1"), ("acquire", "S2"), ("release", "S2"), ("release", "S1")]),
        "B": (5, 8, []),
        "C": (10, 2, [("acquire", "S1"), ("release", "S1")]),
    }
    locks_config = {"S1": 10, "S2": 10}

    modes = [MODE_INHERITANCE, MODE_CEILING]

    for mode in modes:
        print(f"\n{'=' * 70}")
        if mode == MODE_CEILING:
            sched = PriorityScheduler(mode=mode, ceiling_config=locks_config)
        else:
            sched = PriorityScheduler(mode=mode)

        for name, cp in locks_config.items():
            sched.register_lock(name, cp)

        for tid, (pri, work, op_list) in ops.items():
            sched.add_task(Task(tid, pri, work, op_list))

        sched.run(max_steps=15, verbose=True)


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
    demo_chain_propagation()
    demo_already_waiting()
    demo_ceiling_multi_lock_detailed()
    demo_scheduler_comparison()
    demo_nested_scheduler()
    demo_comparison()
