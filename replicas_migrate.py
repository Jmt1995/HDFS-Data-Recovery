from decision import decision_DR_SR, decision_RAR
from models.service import Service
from models.task import Task
from update import *
from utils import *
from models.replica import Replica


def migrate_dead_replicas(Recovery_method, down_node, router, data_list,
                          replicas, services, Link_max_load, D_size,
                          Target_rep_num, beta, max_slot):
    """
    宕机副本迁移
    """

    total_tasks = list()

    pm_id = down_node.id

    if Recovery_method == "DDR":  # 直接恢复
        dead_replicas = list()
        for replica in replicas:
            if pm_id == replica.node_id:
                dead_replicas.append(replica)

        tasks = recovery(Recovery_method, dead_replicas, down_node, router, data_list, replicas,
                 services, max_slot, Link_max_load, D_size)
        total_tasks += tasks

    elif Recovery_method == "SDR":  # 阶段恢复
        dead_replicas = list()
        for replica in replicas:
            if pm_id == replica.node_id:
                dead_replicas.append(replica)

        dead_replicas_last_set = dead_replicas[:]

        for i in range(0, Target_rep_num - 1):
            # 分为 Target_rep_num - 1 个集合
            dead_replicas_set = list()
            for replica in dead_replicas_last_set[:]:
                data_id = replica.data_id
                for data in data_list:
                    if data_id == data.id:
                        if len(data.rep_set) == i + 1:
                            dead_replicas_set.append(replica)
                            dead_replicas_last_set.remove(replica)

            tasks = recovery(Recovery_method, dead_replicas_set, down_node, router, data_list, replicas,
                     services, max_slot, Link_max_load, D_size)
            total_tasks += tasks

        tasks = recovery(Recovery_method, dead_replicas_last_set, down_node, router, data_list, replicas,
                 services, max_slot, Link_max_load, D_size)
        total_tasks += tasks

    elif Recovery_method == "RADR":  # 稀有度感知恢复
        dead_replicas = list()
        for replica in replicas:
            if pm_id == replica.node_id:
                dead_replicas.append(replica)

        recovery_replicas = list()
        for replica in dead_replicas:
            for data in data_list:
                # print("what the fuck")
                if replica.data_id == data.id:
                    # print("find it!")
                    data.update_rarity(beta)
                    if data.rarity <= 0:
                        recovery_replicas.append(replica)

        recovery_prior_replicas = list()
        recovery_secondary_replicas = list()
        for replica in recovery_replicas:
            if isHAR(data_list, replica.data_id, router):
                # 满足 HAR，加入第二队列
                recovery_secondary_replicas.append(replica)
            else:
                # 不满足 HAR，先恢复
                recovery_prior_replicas.append(replica)

        print("prior list len = %d" % len(recovery_prior_replicas))
        print("secondary list len = %d" % len(recovery_secondary_replicas))
        # 先恢复不满足 HAR 的副本
        tasks = recovery(Recovery_method, recovery_prior_replicas, down_node, router, data_list,
                 replicas, services, max_slot, Link_max_load, D_size)
        total_tasks += tasks
        # 再恢复满足 HAR 的副本
        tasks = recovery(Recovery_method, recovery_secondary_replicas, down_node, router, data_list,
                 replicas, services, max_slot, Link_max_load, D_size)
        total_tasks += tasks

    elif Recovery_method == "RASDR":  # 稀有度感知分步恢复
        dead_replicas = list()
        for replica in replicas:
            if pm_id == replica.node_id:
                dead_replicas.append(replica)

        recovery_replicas = list()
        for replica in dead_replicas:
            for data in data_list:
                if replica.data_id == data.id:
                    data.update_rarity(beta)
                    if data.rarity <= 0:
                        recovery_replicas.append(replica)

        recovery_prior_replicas = list()
        recovery_secondary_replicas = list()
        for replica in recovery_replicas:
            if isHAR(data_list, replica.data_id, router):
                # 满足 HAR，加入第二队列
                recovery_secondary_replicas.append(replica)
            else:
                # 不满足 HAR，先恢复
                recovery_prior_replicas.append(replica)

        dead_replicas_last_set = recovery_prior_replicas[:]
        for i in range(0, Target_rep_num - 1):
            # 分为 Target_rep_num - 1 个集合
            dead_replicas_set = list()
            for replica in dead_replicas_last_set[:]:
                data_id = replica.data_id
                for data in data_list:
                    if data_id == data.id:
                        if len(data.rep_set) == i + 1:
                            dead_replicas_set.append(replica)
                            dead_replicas_last_set.remove(replica)

            tasks = recovery(Recovery_method, dead_replicas_set, down_node, router, data_list, replicas,
                     services, max_slot, Link_max_load, D_size)
            total_tasks += tasks

        tasks = recovery(Recovery_method, dead_replicas_last_set, down_node, router, data_list, replicas,
                 services, max_slot, Link_max_load, D_size)
        total_tasks += tasks


        dead_replicas_last_set = recovery_secondary_replicas[:]

        for i in range(0, Target_rep_num - 1):
            # 分为 Target_rep_num - 1 个集合
            dead_replicas_set = list()
            for replica in dead_replicas_last_set[:]:
                data_id = replica.data_id
                for data in data_list:
                    if data_id == data.id:
                        if len(data.rep_set) == i + 1:
                            dead_replicas_set.append(replica)
                            dead_replicas_last_set.remove(replica)

            tasks = recovery(Recovery_method, dead_replicas_set, down_node, router, data_list, replicas,
                     services, max_slot, Link_max_load, D_size)
            total_tasks += tasks

        tasks = recovery(Recovery_method, dead_replicas_last_set, down_node, router, data_list, replicas,
                 services, max_slot, Link_max_load, D_size)
        total_tasks += tasks

        # tasks = recovery(Recovery_method, recovery_prior_replicas, down_node, router, data_list,
        #          replicas, services, max_slot, Link_max_load, D_size)
        # total_tasks += tasks
        #
        # tasks = recovery(Recovery_method, recovery_secondary_replicas, down_node, router, data_list,
        #          replicas, services, max_slot, Link_max_load, D_size)
        total_tasks += tasks


    return total_tasks


def recovery(Recovery_method, dead_replicas, down_node, router, data_list, replicas, services,
             max_slot,
             Link_max_load, D_size):
    """
    副本恢复：先做决策，再进行部署
    """

    print("Len of waiting for recovery replicas: %d" % len(dead_replicas))

    decisions = decide_replicas_migration(Recovery_method, dead_replicas, down_node, replicas,
                                          router, Link_max_load, data_list)

    # print(decisions)
    tasks = deploy_replicas_migration(decisions, router, data_list, replicas,
                                      services, max_slot, Link_max_load, D_size)

    return tasks


def decide_replicas_migration(Recovery_method, dead_replicas, down_node, replicas, router,
                              Link_max_load, data_list):
    """
    副本恢复决策
    """

    if Recovery_method in ("DDR", "SDR"):
        decisions = decision_DR_SR(dead_replicas, down_node, replicas, router,
                                   Link_max_load, data_list)
    elif Recovery_method == "RADR":
        decisions = decision_RAR(dead_replicas, down_node, replicas, router,
                                   Link_max_load, data_list)
    elif Recovery_method == "RASDR":
        decisions = decision_RAR(dead_replicas, down_node, replicas, router,
                                   Link_max_load, data_list)
    else:
        decisions = None

    return decisions


def deploy_replicas_migration(decisions, router, data_list, replicas, services,
                              max_slot, Link_max_load, D_size):
    """
    副本恢复部署，同时更新时间片
    """

    tasks = list()

    # 将任务部署到网络中
    for decision in decisions:
        source_replica = decision.get('source_replica')
        replace_replica = decision.get('replace_replica')
        target_pm = decision.get('target_pm')

        if target_pm and not replace_replica:  # 目的服务器有剩余 slot，不需要替换副本
            new_task = Task(len(tasks), source_replica, gl.time, target_pm,
                            replace_replica)
            tasks.append(new_task)
            add_node_task(new_task, router, "migrate")

        elif replace_replica and not target_pm:  # 需要替换副本
            # 改变副本状态为 dead
            replace_replica.status_dead()
            # 关闭在副本上的服务
            service_in_replica = list()
            for service in services:
                if service.replica_id == replace_replica.id:
                    service.status_dead()
                    service.load = 0
                    service_in_replica.append(service)

            # 找该副本其他的兄弟
            replicas_belongs_to_data = list()
            for data in data_list:
                if data.id == replace_replica.data_id:  # 找到数据
                    # 找该数据的其他活跃副本
                    for _replica in replicas:
                        if _replica.data_id == replace_replica.data_id \
                                and _replica.status == "active":
                            replicas_belongs_to_data.append(_replica)

            # 迁移服务
            for service in service_in_replica:
                # 均分服务
                for _replica in replicas_belongs_to_data:
                    new_service = Service(len(services), _replica.id,
                                          service.desire_load / 2, -1,
                                          service.QoS.copy(), service)
                    _replica.load += service.desire_load / len(
                        replicas_belongs_to_data)
                    add_node_service(router, new_service, _replica)
                    services.append(new_service)

                # 更改服务状态为“已迁移”
                service.status_migrated()

            # 更新物理机 slot 数和被替换副本状态 "replaced"
            replace_replica.status_replaced()
            pm_id = replace_replica.node_id
            for switch in router.children:
                for pm in switch.children:
                    if pm.id == pm_id:
                        pm.add_slot(max_slot)

            # 在网络中添加任务 type = "replace"
            new_task = Task(len(tasks), source_replica, gl.time, target_pm,
                            replace_replica)
            tasks.append(new_task)
            add_node_task(new_task, router, "replace")

    while True:  # 循环执行任务
        # 更新全网链路及服务/任务负载+QoS+HAR等信息
        # router.print_tree()
        print('6666666666666666666666666666666666666666666666666666')
        update_network_load(router, Link_max_load)
        update_data_har(data_list, router)
        update_services_qos(services)

        # 更新 ETA 并计入列表
        # print("Current time = %.1f" % gl.time)
        # print_tasks(tasks)
        eta_tasks = list()
        for task in tasks:
            if task.status == "active":
                eta = task.ETA(D_size)
                eta_tasks.append(eta)

        if len(eta_tasks) == 0:  # ETA 集合为空，则所有迁移任务完成，结束副本迁移程序
            break
        else:  # ETA 集合不为空
            #  找最小 ETA
            print("ETA TASKS = ", end="")
            print(eta_tasks)
            min_eta = min(eta_tasks)
            # 更新任务进度
            for task in tasks:
                if task.status == "active":
                    task.update_process(min_eta, D_size)

            # 跳转时间片
            # if min_eta < 0:
                # print("2333333333333 ETA = %.2f" % min_eta)
            gl.time += min_eta

            # 更新任务 ETA，找到 ETA 小于等于0的任务
            tasks_eta_lt_zero = list()
            for task in tasks:
                if task.status == "active":
                    eta = task.ETA(D_size)
                    if eta <= 0:
                        tasks_eta_lt_zero.append(task)
                        # print("WILL DONE TASK:", end="")
                        # print(task)

            # 结束任务 "done" 并新建新副本
            for task in tasks_eta_lt_zero:
                task.status_done()
                task.load = 0
                task.end_time = gl.time
                # 更新目标物理机 slot 信息
                task.target_pm.del_slot()
                new_replica = Replica(len(replicas), task.source_replica.data_id, task.target_pm.id, 0)
                replicas.append(new_replica)
                # 与 data 建立关联
                for data in data_list:
                    if data.id == new_replica.data_id:
                        data.add_rep(new_replica)
                # print("DONE TASK:", end="")
                # print(task)

    return tasks
