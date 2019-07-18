import global_vars as gl
from har import isHAR


def update_network_load(router, Link_max_load):
    """
    更新全网链路实际负载值和服务/迁移任务的实际负载值
    """

    # 清除网络中所有已迁移的服务和已完成的任务
    # 清空所有之前的 service 和 task 负载信息并重新赋值
    for switch in router.children:
        for service in switch.link.services[:]:
            if service.status != "active":
                switch.link.del_service(service)
                # print("service %d removed in Switch %d" % (service.id, switch.id))
            else:
                service.load = 0
        for task in switch.link.tasks[:]:
            if task.status != "active":
                switch.link.del_task(task)
            else:
                task.load = Link_max_load

        for pm in switch.children:
            for service in pm.link.services[:]:
                if service.status != "active":
                    pm.link.del_service(service)
                    # print("service %d removed in PM %d" % (service.id, pm.id))
                else:
                    service.load = 0

            for task in pm.link.tasks[:]:
                if task.status != "active":
                    pm.link.del_task(task)
                else:
                    task.load = Link_max_load

    for switch in router.children:
        # 更新上层链路
        if len(switch.link.tasks) == 0:  # 上层链路还没有迁移任务，直接计算绑定在该节点上的服务带宽
            switch_services = list(switch.link.services)
            switch_load = 0
            for service in switch.link.services:
                switch_load += service.desire_load

            if switch_load > Link_max_load:  # 超出了最大带宽限制，重新分配带宽
                switch.link.load = Link_max_load

                switch_aver_load = Link_max_load / len(switch_services)
                switch_allocated_load = 0
                last_switch_aver_load = -1

                while True:
                    switch_bt_aver_num = 0
                    for service in switch_services[:]:
                        if service.desire_load <= switch_aver_load:
                            # 服务所需带宽小于等于任务平均带宽，给予分配
                            switch_allocated_load += service.desire_load
                            service.load = service.desire_load
                            switch_services.remove(service)
                            # 更新带宽均值
                            switch_aver_load = \
                                (Link_max_load - switch_allocated_load) \
                                / len(switch_services)
                        else:
                            switch_bt_aver_num += 1

                    # check 一下，看是否小于均值带宽的服务都分到了带宽

                    if last_switch_aver_load == switch_aver_load:
                        # 分配稳定
                        # 按照平均带宽给予分配
                        for service in switch_services:
                            if service.load > service.desire_load:
                                print("||||| load = %.1f, desire load = %.1f" % (service.load, service.desire_load))
                            service.load = switch_aver_load
                        break

                    last_switch_aver_load = switch_aver_load

            else:  # 没有超出限制，更新服务和上层的带宽负载
                switch.link.load = switch_load
                for service in switch.link.services:
                    service.load = service.desire_load

        else:  # 上层链路有迁移任务，需要检查源和目的节点的路径
            # 链路之间独立
            switch_services = list(switch.link.services)
            switch_tasks = list(switch.link.tasks)
            switch.link.load = Link_max_load  # 只要有迁移任务，肯定是占满带宽
            switch_allocated_load = 0
            switch_aver_load = Link_max_load / (len(switch_services) + len(switch_tasks))
            last_switch_aver_load = -1

            while True:
                switch_bt_aver_num = 0  # 所需带宽大于任务平均带宽的服务数
                for service in switch_services[:]:
                    if service.desire_load <= switch_aver_load:
                        # 服务所需带宽小于等于任务平均带宽，给予分配
                        switch_allocated_load += service.desire_load
                        service.load = service.desire_load
                        switch_services.remove(service)
                        # 更新带宽均值
                        switch_aver_load = \
                            (Link_max_load - switch_allocated_load) \
                            / (len(switch_services) + len(switch_tasks))
                    else:
                        switch_bt_aver_num += 1
                if switch_aver_load == last_switch_aver_load:
                    # 未分配带宽的任务需求带宽都大于它们所获得平均带宽
                    # 对服务和任务都按照平均带宽给予分配
                    for service in switch_services:
                        service.load = switch_aver_load
                        if service.load > service.desire_load:
                            print("|||||-|| load = %.1f, desire load = %.1f" % (
                            service.load, service.desire_load))

                    for task in switch_tasks:
                        if task.load > switch_aver_load:
                            task.load = switch_aver_load
                    break

                last_switch_aver_load = switch_aver_load

        for pm in switch.children:
            # 更新下层链路
            if len(pm.link.tasks) == 0:  # 下层链路没有迁移任务，直接从上层链路计算的结果更新
                pm_load = 0
                for service in pm.link.services:
                    pm_load += service.load
                pm.link.load = pm_load

            else:  # 下层链路有迁移任务，需要检查源和目的节点的路径
                pm_services = list(pm.link.services)
                pm_tasks = list(pm.link.tasks)
                # print("PM Service len = %d, PM id = %d" % (len(pm_services), pm.id))
                # print("PM Tasks len = %d, PM id = %d" % (len(pm_tasks), pm.id))
                pm.link.load = Link_max_load  # 只要有迁移任务，肯定是占满带宽
                pm_allocated_load = 0
                pm_aver_load = Link_max_load / (len(pm_services) + len(pm_tasks))
                last_pm_aver_load = -1

                while True:
                    pm_bt_aver_num = 0  # 上层链路获得带宽大于任务平均带宽的任务数
                    for service in pm_services[:]:
                        if service.load <= pm_aver_load:
                            # 服务在上层链路获得带宽小于等于任务平均带宽，给予分配
                            pm_allocated_load += service.load
                            pm_services.remove(service)
                            # 更新带宽均值
                            pm_aver_load = \
                                (Link_max_load - pm_allocated_load) \
                                / (len(pm_services) + len(pm_tasks))
                        else:
                            # 服务在上层链路获得带宽大于任务平均带宽
                            pm_bt_aver_num += 1

                    # check 一下，看是否小于均值带宽的服务都分到了带宽

                    if pm_aver_load == last_pm_aver_load:
                        # 分配稳定
                        # 对服务和任务都按照平均带宽给予分配，同时更新服务实际获得带宽
                        for service in pm_services:
                            service.load = pm_aver_load
                            if service.load > service.desire_load:
                                print("||||| load = %.1f, desire load = %.1f" % (service.load, service.desire_load))
                        for task in pm_tasks:
                            # print("44444444Task load = %.1f" % task.load)
                            # print(task)
                            if task.load > pm_aver_load:
                                task.load = pm_aver_load
                                # print("4444442333Task load = %.1f" % task.load)
                        break

                    last_pm_aver_load = pm_aver_load


def update_services_qos(services):
    for service in services:
        if service.status == "active":
            service.QoS[gl.time] = service.load * 100 / service.desire_load


def update_data_har(data_list, router):
    for data in data_list:
        data.HAR[gl.time] = isHAR(data_list, data.id, router)

def update_task_process(tasks, D_size):
    for task in tasks:
        if task.status == "active":
            task.update_process(D_size)


def add_node_service(router, service, replica):
    """
    将新建服务添加到节点上(更新 Link 信息)
    """

    pm_id = replica.node_id

    for switch in router.children:
        for pm in switch.children:
            if pm.id == pm_id:
                pm.link.add_service(service)
                switch.link.add_service(service)

    return router


def add_node_task(task, router, add_type):
    """
    将新建迁移任务添加到节点上(更新 Link 信息)
    """

    # 找源与目的的机器
    if add_type == "migrate":  # 目的服务器有剩余 slot，不需要替换副本
        source_pm = None
        target_pm = task.target_pm

        for switch in router.children:
            for pm in switch.children:
                if task.source_replica.node_id == pm.id:
                    source_pm = pm

        if source_pm:
            if source_pm.parent == target_pm.parent:  # 在同一个机架(交换机)下
                # 将任务加载到 PM 的链路上
                source_pm.link.add_task(task)
                target_pm.link.add_task(task)
            else:  # 在不同机架(交换机)下
                # 将任务加载到 PM 及其 Switch 的链路上
                source_pm.link.add_task(task)
                target_pm.link.add_task(task)
                source_pm.parent.link.add_task(task)
                target_pm.parent.link.add_task(task)
        else:
            print("[ERROR] Cannot find the source PM in this decision!")

    elif add_type == "replace":  # 需要替换副本
        source_pm = None
        target_pm = None

        for switch in router.children:
            for pm in switch.children:
                if pm.id == task.source_replica.node_id:
                    source_pm = pm
                elif pm.id == task.replace_replica.node_id:
                    target_pm = pm

        # 更新目标主机信息
        task.target_pm = target_pm

        if source_pm and target_pm:
            if source_pm.parent == target_pm.parent:  # 在同一个机架(交换机)下
                # 将任务加载到 PM 的链路上
                source_pm.link.add_task(task)
                target_pm.link.add_task(task)
            else:  # 在不同机架(交换机)下
                # 将任务加载到 PM 及其 Switch 的链路上
                source_pm.link.add_task(task)
                target_pm.link.add_task(task)
                source_pm.parent.link.add_task(task)
                target_pm.parent.link.add_task(task)
        else:
            print("[ERROR] Cannot find the source PM in this decision!")
