import random
import global_vars as gl


def seek_incident_break_node_id(router):
    """
    选中某一台服务器宕机
    :param router:
    :return:
    """

    # 所有的可用服务器
    available_pms = list()

    for switch in router.children:
        for pm in switch.children:
            available_pms.append(pm)

    chosen_break_node = random.choice(available_pms)

    return chosen_break_node.id


def incident_break(router, pm_id):
    """
    更新宕机机器状态
    :param router:
    :param pm_id:
    :return:
    """
    for switch in router.children:
        for pm in switch.children:
            if pm.id == pm_id:
                pm.status_down()
                return pm


def update_break_status(down_node, data_list, replicas, services):
    """
    更新服务负载和数据副本集状态
    """

    pm_id = down_node.id

    for replica in replicas:
        if pm_id == replica.node_id:
            replica.status_dead()
            replica_id = replica.id
            data_id = replica.data_id
            for service in services:
                if service.replica_id == replica_id:
                    service.status_dead()
                    service.load = 0
            for data in data_list:
                if data.id == data_id:
                    data.del_rep(replica)
                    # print("replica deleted in data rep_set")

    return True
