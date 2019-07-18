import random
import global_vars as gl

from har import isHAR
from models.data import Data
from models.node import Node
from models.replica import Replica
from models.service import Service


def init_nodes(N_rack, N_r_pm, max_slot):
    node_id = 0
    router = Node(node_id)
    node_id += 1
    # Switches

    for i in range(0, N_rack):
        router.add_child(node_id)
        node_id += 1

    # PMs

    switches = router.children
    for switch in switches:
        for i in range(0, N_r_pm):
            switch.add_child(node_id, max_slot)
            node_id += 1

    return router


def init_data(D_type):
    data_list = list()

    for i in range(0, D_type):
        data_list.append(Data(i))

    return data_list


def init_replicas(data_list, Rep_set_range, last_replica_id, router):
    _min = Rep_set_range.get("min")
    _max = Rep_set_range.get("max")
    aver = (_max + _min) / 2
    sigma = (aver - _min) / 2  # 95% 的面积

    replicas = list()

    for data in data_list:
        num = int(round(random.gauss(aver, sigma)))
        if num < _min:
            num = _min
        elif num > _max:
            num = _max

        for replica_id in range(last_replica_id, last_replica_id + num):
            chosen_node_id = seek_node_init(data_list, router, data.id)
            new_replica = Replica(replica_id, data.id, chosen_node_id, 0)
            replicas.append(new_replica)
            data.add_rep(new_replica)  # Update data
            deploy_replica_init(router, chosen_node_id)  # Deploy

        last_replica_id += num

    return replicas


def init_services(router, replicas, data_list, Service_load_range,
                  last_service_id, u, Link_max_load, max_slot):
    _min = Service_load_range.get("min")
    _max = Service_load_range.get("max")
    aver = (_min + _max) / 2
    sigma = (aver - _min) / 2  # 95% 的面积

    services = list()
    up_link_usage = 0

    while up_link_usage < u:

        load = int(round(random.gauss(aver, sigma)))
        if load < _min:
            load = _min
        elif load > _max:
            load = _max
        desire_load = load

        context = seek_replica_init(router, replicas, Service_load_range,
                                    Link_max_load, max_slot)
        chosen_replica = context.get("chosen_replica")
        min_up_load = context.get("min_up_load")
        min_down_load = context.get("min_down_load")

        if load < Link_max_load - min_up_load and load < Link_max_load - min_down_load:
            # 分配服务
            new_service = Service(last_service_id, chosen_replica.id,
                                  desire_load, load)
            services.append(new_service)
            last_service_id += 1
            update_link_load_init(new_service.id, services, replicas, data_list,
                                  router)
        else:
            print('There is no more bandwidth for service!')
            break

        # Update up link usage value
        up_link_usage = update_up_link_usage(router, Link_max_load)
        print('Up link usage = ' + str(up_link_usage) + "%")

    return services


def seek_node_init(data_list, router, data_id):
    """
    为副本寻找能够部署的服务器
    :param data_list:
    :param router:
    :param data_id:
    :return: 已找到合适的机器 id
    """

    for data in data_list:
        if data.id == data_id:

            exist_pms = list()
            for rep in data.rep_set:
                exist_pms.append(rep.node_id)

            if len(exist_pms) == 0:  # 不存在副本，找一个最多 slot 的服务器放
                max_slot = 0
                node_id = -1
                for switch in router.children:
                    for pm in switch.children:
                        if pm.slot > max_slot:
                            max_slot = pm.slot
                            node_id = pm.id

                if node_id != -1:
                    return node_id
                else:
                    print("There is no more free slot for replica!")
                    exit(1)

            else:  # 已经存在副本，需要在满足 HAR 情况下寻找服务器
                if isHAR(data_list, data.id,
                         router):  # 满足了 HAR，找一个slot最多的(只要不在同一台物理机即可)
                    max_slot = 0
                    node_id = -1
                    for switch in router.children:
                        for pm in switch.children:
                            if pm.slot > max_slot and pm.id not in exist_pms:
                                max_slot = pm.slot
                                node_id = pm.id

                    if node_id != -1:
                        return node_id
                    else:
                        print("There is no more free slot for replica!")
                        exit(1)

                else:  # 不满足 HAR，需要找其他 Rack
                    # Find current rack
                    cur_rack = router
                    max_slot = 0
                    node_id = -1
                    for switch in router.children:
                        for pm in switch.children:
                            if pm.id in exist_pms:
                                cur_rack = pm.parent

                    for switch in router.children:
                        if switch == cur_rack:
                            continue
                        else:
                            for pm in switch.children:
                                if pm.slot > max_slot:
                                    max_slot = pm.slot
                                    node_id = pm.id

                    if node_id != -1:
                        return node_id
                    else:
                        print("There is no more free slot for replica!")
                        exit(1)


def deploy_replica_init(router, node_id):
    """
    部署副本
    :param router:
    :param node_id:
    :return:
    """
    deployed = False
    for switch in router.children:
        for pm in switch.children:
            if node_id == pm.id:
                pm.slot -= 1
                deployed = True
                break
        if deployed:
            break

    return True


def seek_replica_init(router, replicas, Service_load_range, Link_max_load,
                      max_slot):
    """
    为服务寻找能够提供服务的副本
    :param replicas:
    :param Service_load_range:
    :return: 最佳提供服务的副本， 最小负载上层链路和最小负载下层链路的值
    """

    # 找负载最小的上层链路
    min_up_load = Link_max_load
    switch_id = -1
    for switch in router.children:
        if switch.link.load < min_up_load:
            min_up_load = switch.link.load
            switch_id = switch.id

    # 找下层链路负载最小且有数据块的机器
    min_down_load = Link_max_load
    pm_id = -1
    for switch in router.children:
        if switch_id == switch.id:
            for pm in switch.children:
                if pm.slot < max_slot and pm.link.load < min_down_load:
                    min_down_load = pm.link.load
                    pm_id = pm.id

    # 找这个机器内可以提供服务的副本集合
    chosen_replicas = list()
    for replica in replicas:
        if replica.node_id == pm_id:
            chosen_replicas.append(replica)

    # 随机选一个可选用的服务
    chosen_replica = random.choice(chosen_replicas)

    context = {
        "chosen_replica": chosen_replica,
        "min_up_load": min_up_load,
        "min_down_load": min_down_load,
    }

    return context


def update_link_load_init(service_id, services, replicas, data_list, router):
    """
    更新网络负载值，生成初始 QoS 值
    :param service_id:
    :param services:
    :param replicas:
    :param data_list:
    :param router:
    :return:
    """

    # 找唯一的副本 id & 提取服务负载值 & 生成初始 QoS 值
    for service in services:
        if service.id == service_id:
            replica_id = service.replica_id
            service_load = service.load
            service.QoS[gl.time] = service.load * 100 / service.desire_load
            new_service = service
            break

    # 找唯一的数据 id & 机器 id & 更新副本负载值
    for replica in replicas:
        if replica.id == replica_id:
            replica.load += service_load
            data_id = replica.data_id
            pm_id = replica.node_id
            break

    # 更新数据负载值
    for data in data_list:
        if data.id == data_id:
            if data.degree == -1:
                data.degree = service_load
            else:
                data.degree += service_load
            break

    # 更新当前网络负载值
    isBreak = False
    for switch in router.children:
        if isBreak:
            break
        for pm in switch.children:
            if pm.id == pm_id:
                if new_service:
                    switch.link.load += service_load
                    switch.link.add_service(new_service)

                    pm.link.load += service_load
                    pm.link.add_service(new_service)

                    isBreak = True
                    break


def update_up_link_usage(router, Link_max_load):
    up_link_load_sum = 0

    for switch in router.children:
        up_link_load_sum += switch.link.load

    n = len(router.children)

    up_link_usage = up_link_load_sum * 100 / (n * Link_max_load)

    return up_link_usage
