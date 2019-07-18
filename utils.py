import copy


def print_data(data_list):
    for data in data_list:
        print(data)
        for replica in data.rep_set:
            print("\t", end="")
            print(replica)
        for t, har in data.HAR.items():
            print("\tTime = %.1fs, \tHAR = %s" % (t, har))


def print_replicas(replicas):
    for replica in replicas:
        print(replica)


def print_services(services):
    for service in services:
        print(service)
        qos = service.get_service_qos()
        for t, q in qos.items():
            print("\tTime = %.1fs, \tQoS = %.1f" % (t, q) + "%")


def print_tasks(tasks):
    if tasks:
        for task in tasks:
            print(task)


def get_down_node(router, down_node_id):
    for switch in router.children:
        for pm in switch.children:
            if pm.id == down_node_id:
                return pm
    return False


def copy_whole_network(router, data_list, replicas, services):
    router_copy = copy.deepcopy(router)
    data_list_copy = copy.deepcopy(data_list)
    replicas_copy = set()
    for data in data_list_copy:  # 添加活动状态的副本
        for replica in data.rep_set:
            replicas_copy.add(replica)

    for replica in replicas:  # 添加失效以及其他状态的副本
        if replica.status != "active":
            replicas_copy.add(copy.deepcopy(replica))

    services_copy = set()
    for switch in router_copy.children:  # 添加活动状态的服务
        for pm in switch.children:
            for service in pm.link.services:
                services_copy.add(service)

    for service in services:
        if service.status != "active":
            services_copy.add(copy.deepcopy(service))

    replicas_copy = list(replicas_copy)
    services_copy = list(services_copy)

    return router_copy, data_list_copy, replicas_copy, services_copy
