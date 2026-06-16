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
from scheduler import Task, PriorityScheduler, MODE_NONE, MODE_INHERITANCE, MODE_CEILING


passed = 0
failed = 0


def check(test_name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {test_name}")
    else:
        failed += 1
        print(f"  ❌ {test_name}  {detail}")


def test_duplicate_wait():
    print("\n[1] 重复等待：同一任务在等待期间多次 acquire 返回 already_waiting")
    lock = PriorityInheritanceLock("T1")
    lock.acquire("A", 1)

    thread_r = []
    def wait_thread():
        r = lock.acquire("B", 5, timeout=2.0)
        thread_r.append(r)

    t = threading.Thread(target=wait_thread)
    t.start()
    time.sleep(0.1)

    r2 = lock.acquire("B", 5, timeout=0.1)
    r3 = lock.acquire("B", 5, timeout=0.1)

    lock.release("A")
    t.join()
    r1 = thread_r[0]

    check("第1次（线程中）获取成功",
          r1 == ACQUIRE_OK,
          f"got {r1}")
    check("第2次返回 already_waiting",
          r2 == ALREADY_WAITING,
          f"got {r2}")
    check("第3次返回 already_waiting",
          r3 == ALREADY_WAITING,
          f"got {r3}")

    lock.release("B")


def test_ceiling_already_waiting():
    print("\n[2] 天花板锁重复等待：等待期间返回 already_waiting")
    protocol = PriorityCeilingProtocol()
    lock = protocol.register_lock("A", 10)
    lock.acquire("X", 1)

    thread_r = []
    def wait_thread():
        r = lock.acquire("Y", 5, timeout=2.0)
        thread_r.append(r)

    t = threading.Thread(target=wait_thread)
    t.start()
    time.sleep(0.1)

    r2 = lock.acquire("Y", 5, timeout=0.1)
    r3 = lock.acquire("Y", 5, timeout=0.1)

    lock.release("X")
    t.join()
    r1 = thread_r[0]

    check("第1次（线程中）获取成功", r1 == ACQUIRE_OK, f"got {r1}")
    check("第2次返回 already_waiting", r2 == ALREADY_WAITING, f"got {r2}")
    check("第3次返回 already_waiting", r3 == ALREADY_WAITING, f"got {r3}")

    lock.release("Y")


def test_chain_propagation():
    print("\n[3] 嵌套锁：优先级沿等待链传递到链尾")
    graph = InheritanceGraph()
    s1 = PriorityInheritanceLock("S1", graph=graph)
    s2 = PriorityInheritanceLock("S2", graph=graph)

    s1.acquire("A", 1)
    s2.acquire("B", 5)

    check("S1初始有效优先级=1", s1.owner_effective_priority == 1,
          f"got {s1.owner_effective_priority}")
    check("S2初始有效优先级=5", s2.owner_effective_priority == 5,
          f"got {s2.owner_effective_priority}")

    def a_waits_s2():
        s2.acquire("A", 1)
        time.sleep(0.05)
        s2.release("A")
        s1.release("A")

    t_a = threading.Thread(target=a_waits_s2)
    t_a.start()
    time.sleep(0.1)

    check("A等待S2后 S1持有者A仍有效=1", s1.owner_effective_priority == 1,
          f"got {s1.owner_effective_priority}")
    check("A等待S2后 S2持有者B仍有效=5", s2.owner_effective_priority == 5,
          f"got {s2.owner_effective_priority}")

    def c_waits_s1():
        s1.acquire("C", 10)
        time.sleep(0.05)
        s1.release("C")

    t_c = threading.Thread(target=c_waits_s1)
    t_c.start()
    time.sleep(0.2)

    check("C等待S1后 S1持有者A优先级提升到10",
          s1.owner_effective_priority == 10,
          f"got {s1.owner_effective_priority}")
    check("C等待S1后 S2持有者B优先级也被提升到10",
          s2.owner_effective_priority == 10,
          f"got {s2.owner_effective_priority}")

    s2.release("B")
    t_a.join(timeout=2)
    t_c.join(timeout=2)

    check("释放后S1无持有者", s1.owner is None, f"got {s1.owner}")
    check("释放后S2无持有者", s2.owner is None, f"got {s2.owner}")


def test_chain_print():
    print("\n[4] 等待链可视化打印")
    graph = InheritanceGraph()
    s1 = PriorityInheritanceLock("S1", graph=graph)
    s2 = PriorityInheritanceLock("S2", graph=graph)

    s1.acquire("A", 1)
    s2.acquire("B", 5)

    chain_str = None
    try:
        graph.print_chain(s1, "    ")
        chain_ok = True
    except Exception as e:
        chain_ok = False
        print(f"  打印失败: {e}")

    check("等待链打印无异常", chain_ok)


def test_wrong_release():
    print("\n[5] 错误释放：非持有者释放应抛出异常")
    lock = PriorityInheritanceLock("T2")
    lock.acquire("A", 1)
    try:
        lock.release("B")
        check("非持有者释放应抛异常", False, "未抛出异常")
    except RuntimeError as e:
        has_msg = "不是" in str(e) and "持有者" in str(e)
        check("非持有者释放抛出 RuntimeError", has_msg, str(e))

    lock.release("A")
    try:
        lock.release("A")
        check("释放已释放的锁应抛异常", False, "未抛出异常")
    except RuntimeError:
        check("重复释放抛出 RuntimeError", True)


def test_release_ownership_transfer():
    print("\n[6] 释放归属：释放后锁只转交一个等待者")
    lock = PriorityInheritanceLock("T3")
    lock.acquire("A", 1)

    results = {}
    barrier = threading.Barrier(3, timeout=5)

    def waiter(tid, prio):
        r = lock.acquire(tid, prio, timeout=5)
        results[tid] = r
        if r == ACQUIRE_OK:
            time.sleep(0.05)
            lock.release(tid)

    threads = [
        threading.Thread(target=waiter, args=("B", 5)),
        threading.Thread(target=waiter, args=("C", 8)),
        threading.Thread(target=waiter, args=("D", 3)),
    ]

    for t in threads:
        t.start()
    time.sleep(0.2)

    lock.release("A")

    for t in threads:
        t.join(timeout=5)

    owners_after = [tid for tid, r in results.items() if r == ACQUIRE_OK]
    check("三个等待者都获得了锁（依次）", len(owners_after) == 3,
          f"got {len(owners_after)}: {owners_after}")

    order = list(results.keys())
    priority_order = sorted(order, key=lambda x: {"B": 5, "C": 8, "D": 3}[x], reverse=True)
    check("等待者按优先级高低依次获取", order == priority_order,
          f"got {order}, expected {priority_order}")


def test_timeout_wait():
    print("\n[7] 超时等待：超时后返回 timeout，锁状态不变")
    lock = PriorityInheritanceLock("T4")
    lock.acquire("A", 1)

    r = lock.acquire("B", 5, timeout=0.2)
    check("超时返回 ACQUIRE_TIMEOUT", r == ACQUIRE_TIMEOUT, f"got {r}")
    check("超时后锁持有者不变", lock.owner == "A", f"got {lock.owner}")

    lock.release("A")
    time.sleep(0.1)
    check("超时后释放锁，无等待者接管", lock.owner is None, f"got {lock.owner}")


def test_cancel_wait():
    print("\n[8] 取消等待：cancel 后任务收到 cancelled 结果")
    lock = PriorityInheritanceLock("T5")
    lock.acquire("A", 1)

    result = [None]

    def waiter():
        result[0] = lock.acquire("B", 5, timeout=5)

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.1)

    ok = lock.cancel("B")
    check("cancel 返回 True", ok, f"got {ok}")
    t.join(timeout=2)
    check("被取消任务收到 ACQUIRE_CANCELLED", result[0] == ACQUIRE_CANCELLED,
          f"got {result[0]}")

    lock.release("A")
    time.sleep(0.1)
    check("取消后锁持有者为 None", lock.owner is None, f"got {lock.owner}")


def test_inheritance_boost_and_restore():
    print("\n[9] 优先级继承提升与恢复")
    lock = PriorityInheritanceLock("T6")
    lock.acquire("L", 1)
    check("初始有效优先级为 1", lock.owner_effective_priority == 1,
          f"got {lock.owner_effective_priority}")

    def h_wait():
        lock.acquire("H", 10, timeout=5)
        time.sleep(0.05)
        lock.release("H")

    t = threading.Thread(target=h_wait)
    t.start()
    time.sleep(0.1)

    check("H 等待后 L 优先级提升到 10",
          lock.owner_effective_priority == 10,
          f"got {lock.owner_effective_priority}")

    lock.release("L")
    t.join(timeout=3)
    time.sleep(0.1)

    check("L 释放后锁无持有者", lock.owner is None, f"got {lock.owner}")


def test_ceiling_multi_lock_blocked():
    print("\n[10] 多锁天花板：不符合规则的获取被阻止")
    protocol = PriorityCeilingProtocol()
    lock_a = protocol.register_lock("A", 5)
    lock_b = protocol.register_lock("B", 10)

    r = lock_a.acquire("L", 1)
    check("L 获取锁 A (天花板5)", r == ACQUIRE_OK, f"got {r}")
    check("系统天花板 = 5", protocol.system_ceiling() == 5,
          f"got {protocol.system_ceiling()}")

    r = lock_b.acquire("M", 3)
    check("M(优先级3) 获取锁 B 被拒绝 (3 <= 天花板5)", r == ACQUIRE_BLOCKED,
          f"got {r}")

    reason = protocol.explain_blocked("M", 3, "B")
    check("explain_blocked 返回正确原因", "天花板=5" in reason and "≥" in reason and "3" in reason,
          f"got {reason}")

    r = lock_b.acquire("H", 8)
    check("H(优先级8) 获取锁 B 允许 (8 > 天花板5)", r == ACQUIRE_OK,
          f"got {r}")

    lock_b.release("H")
    lock_a.release("L")

    r = lock_b.acquire("M", 3)
    check("L释放后 M 可以获取锁 B", r == ACQUIRE_OK, f"got {r}")
    lock_b.release("M")


def test_ceiling_wrong_release():
    print("\n[11] 天花板锁错误释放")
    protocol = PriorityCeilingProtocol()
    lock = protocol.register_lock("X", 10)
    lock.acquire("A", 1)

    try:
        lock.release("B")
        check("非持有者释放天花板锁应抛异常", False)
    except RuntimeError:
        check("非持有者释放天花板锁抛出 RuntimeError", True)

    lock.release("A")


def test_ceiling_system_ceiling_dynamic():
    print("\n[12] 系统天花板动态变化")
    protocol = PriorityCeilingProtocol()
    lock_a = protocol.register_lock("A", 3)
    lock_b = protocol.register_lock("B", 7)
    lock_c = protocol.register_lock("C", 10)

    check("无锁占用时系统天花板 = 0", protocol.system_ceiling() == 0,
          f"got {protocol.system_ceiling()}")

    lock_a.acquire("L", 1)
    check("锁A(天花板3)被占用 → 系统天花板 = 3", protocol.system_ceiling() == 3,
          f"got {protocol.system_ceiling()}")

    lock_b.acquire("L", 1)
    check("锁A+B被占用 → 系统天花板 = min(3,7) = 3", protocol.system_ceiling() == 3,
          f"got {protocol.system_ceiling()}")

    lock_a.release("L")
    check("锁A释放 → 系统天花板 = 7 (只剩B)", protocol.system_ceiling() == 7,
          f"got {protocol.system_ceiling()}")

    lock_b.release("L")
    check("所有锁释放 → 系统天花板 = 0", protocol.system_ceiling() == 0,
          f"got {protocol.system_ceiling()}")


def test_ceiling_same_task_multi_lock():
    print("\n[13] 同一任务持有多个天花板锁")
    protocol = PriorityCeilingProtocol()
    lock_a = protocol.register_lock("A", 5)
    lock_b = protocol.register_lock("B", 10)

    r1 = lock_a.acquire("T", 3)
    r2 = lock_b.acquire("T", 3)
    check("同一任务可以持有多个锁", r1 == ACQUIRE_OK and r2 == ACQUIRE_OK,
          f"got {r1}, {r2}")
    check("任务持有两把锁", len(protocol.get_held_locks("T")) == 2,
          f"got {len(protocol.get_held_locks('T'))}")

    lock_b.release("T")
    check("释放一把后还剩一把", len(protocol.get_held_locks("T")) == 1,
          f"got {len(protocol.get_held_locks('T'))}")

    lock_a.release("T")
    check("全部释放后无持有", len(protocol.get_held_locks("T")) == 0,
          f"got {len(protocol.get_held_locks('T'))}")


def test_ceiling_blocked_vs_timeout():
    print("\n[14] 天花板锁：被规则拒绝 vs 持锁超时")
    protocol = PriorityCeilingProtocol()
    lock_a = protocol.register_lock("A", 5)
    lock_b = protocol.register_lock("B", 10)

    lock_a.acquire("X", 1)
    r = lock_b.acquire("Y", 3, timeout=0.2)
    check("被天花板规则拒绝返回 ACQUIRE_BLOCKED", r == ACQUIRE_BLOCKED, f"got {r}")

    lock_a.release("X")

    lock_b.acquire("Z", 1)
    r = lock_b.acquire("Y", 5, timeout=0.2)
    check("锁被他人持有时超时返回 ACQUIRE_TIMEOUT", r == ACQUIRE_TIMEOUT, f"got {r}")

    lock_b.release("Z")


def test_reapply_inheritance_on_cancel():
    print("\n[15] 取消等待后优先级恢复")
    graph = InheritanceGraph()
    lock = PriorityInheritanceLock("T12", graph=graph)
    lock.acquire("L", 1)

    mid_result = [None]

    def mid_wait():
        r = lock.acquire("M", 5, timeout=5)
        mid_result[0] = r
        if r == ACQUIRE_OK:
            time.sleep(0.05)
            lock.release("M")

    high_result = [None]

    def high_wait():
        r = lock.acquire("H", 10, timeout=5)
        high_result[0] = r
        if r == ACQUIRE_OK:
            time.sleep(0.05)
            lock.release("H")

    t_m = threading.Thread(target=mid_wait)
    t_m.start()
    time.sleep(0.1)

    t_h = threading.Thread(target=high_wait)
    t_h.start()
    time.sleep(0.1)

    check("L 继承到最高等待者优先级 10",
          lock.owner_effective_priority == 10,
          f"got {lock.owner_effective_priority}")

    lock.cancel("H")
    t_h.join(timeout=2)
    time.sleep(0.1)
    check("取消 H 后 L 有效优先级降到 M 的 5",
          lock.owner_effective_priority == 5,
          f"got {lock.owner_effective_priority}")

    lock.cancel("M")
    t_m.join(timeout=2)
    time.sleep(0.1)
    check("取消 M 后 L 有效优先级恢复原始 1",
          lock.owner_effective_priority == 1,
          f"got {lock.owner_effective_priority}")

    lock.release("L")


def test_scheduler_basic():
    print("\n[16] 调度器：三种模式下任务状态表正确")
    ops = {
        "L": (1, 3, [("acquire", "S"), ("release", "S")]),
        "H": (10, 2, [("acquire", "S"), ("release", "S")]),
    }
    locks_config = {"S": 10}

    for mode in [MODE_NONE, MODE_INHERITANCE, MODE_CEILING]:
        if mode == MODE_CEILING:
            sched = PriorityScheduler(mode=mode, ceiling_config=locks_config)
        else:
            sched = PriorityScheduler(mode=mode)

        for name, cp in locks_config.items():
            sched.register_lock(name, cp)

        for tid, (pri, work, op_list) in ops.items():
            sched.add_task(Task(tid, pri, work, op_list))

        sched.run(max_steps=20, verbose=False)
        summary = sched.get_summary()

        check(f"[{mode}] 调度器完成", summary["total_time"] > 0,
              f"total_time={summary['total_time']}")


def test_scheduler_comparison():
    print("\n[17] 调度器：三种模式对比下高优任务等待时间差异")
    ops = {
        "L": (1, 3, [("acquire", "S"), ("release", "S")]),
        "M": (5, 5, []),
        "H": (10, 2, [("acquire", "S"), ("release", "S")]),
    }
    locks_config = {"S": 10}

    wait_times = {}

    for mode in [MODE_NONE, MODE_INHERITANCE, MODE_CEILING]:
        if mode == MODE_CEILING:
            sched = PriorityScheduler(mode=mode, ceiling_config=locks_config)
        else:
            sched = PriorityScheduler(mode=mode)

        for name, cp in locks_config.items():
            sched.register_lock(name, cp)

        for tid, (pri, work, op_list) in ops.items():
            sched.add_task(Task(tid, pri, work, op_list))

        sched.run(max_steps=30, verbose=False)
        summary = sched.get_summary()

        for td in summary["tasks"]:
            if td["id"] == "H":
                wait_times[mode] = td["wait_time"]

    check("无保护模式 H 等待时间最长",
          wait_times[MODE_NONE] >= wait_times[MODE_INHERITANCE],
          f"none={wait_times[MODE_NONE]}, inheritance={wait_times[MODE_INHERITANCE]}")
    check("继承模式优于无保护",
          wait_times[MODE_INHERITANCE] <= wait_times[MODE_NONE],
          f"inheritance={wait_times[MODE_INHERITANCE]}, none={wait_times[MODE_NONE]}")


def test_three_lock_ceiling():
    print("\n[18] 三锁天花板：多锁占用时拦截依据一致")
    protocol = PriorityCeilingProtocol()
    lock_x = protocol.register_lock("X", 3)
    lock_y = protocol.register_lock("Y", 7)
    lock_z = protocol.register_lock("Z", 10)

    r = lock_x.acquire("L", 1)
    check("L 获取 X 成功", r == ACQUIRE_OK, f"got {r}")
    check("系统天花板 = min(3) = 3", protocol.system_ceiling() == 3,
          f"got {protocol.system_ceiling()}")

    r = lock_y.acquire("M", 5)
    check("M(5) 获取 Y 成功（5 > 其他被占用锁天花板3）", r == ACQUIRE_OK, f"got {r}")
    check("系统天花板 = min(3,7) = 3", protocol.system_ceiling() == 3,
          f"got {protocol.system_ceiling()}")

    r = lock_z.acquire("H", 8)
    check("H(8) 获取 Z 成功（8 > 其他被占用锁天花板3）", r == ACQUIRE_OK, f"got {r}")

    debug = protocol.debug_state()
    check("debug_state 包含所有锁信息",
          "X" in debug and "Y" in debug and "Z" in debug,
          "缺少锁信息")

    lock_z.release("H")
    lock_y.release("M")
    lock_x.release("L")


if __name__ == "__main__":
    print("=" * 60)
    print("优先级反转防护锁 — 测试套件")
    print("=" * 60)

    test_duplicate_wait()
    test_ceiling_already_waiting()
    test_chain_propagation()
    test_chain_print()
    test_wrong_release()
    test_release_ownership_transfer()
    test_timeout_wait()
    test_cancel_wait()
    test_inheritance_boost_and_restore()
    test_ceiling_multi_lock_blocked()
    test_ceiling_wrong_release()
    test_ceiling_system_ceiling_dynamic()
    test_ceiling_same_task_multi_lock()
    test_ceiling_blocked_vs_timeout()
    test_reapply_inheritance_on_cancel()
    test_scheduler_basic()
    test_scheduler_comparison()
    test_three_lock_ceiling()

    print("\n" + "=" * 60)
    print(f"测试结果: {passed} 通过, {failed} 失败, 共 {passed + failed} 项")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)
