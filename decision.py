import copy

from har import isHAR


def decision_DR_SR(recover_list, down_node, replicas, router, Link_max_load, data_list):
    router_copy = copy.deepcopy(router)
    results = list()

    # pm_id = down_node.id

    # Simulate Data
    # recover_list = list()
    # for replica in replicas:
    #     if pm_id == replica.node_id:  # need to recovery
    #         recover_list.append(replica)



    # recover_list 即为接受的参数
    print("number of data: %d" % len(recover_list))

    for recover_rep in recover_list:
        recover_success_flag = False

        source_switch = None
        source_replica = recover_rep
        destination_replica = None
        reset_pm(down_node, router_copy)
        data_id = recover_rep.data_id
        source_node_list = list()
        for replica in replicas:
            if replica.status == "active" and replica.data_id == data_id:
                # find the source
                for switch in router_copy.children:
                    for pm in switch.children:
                        if pm.id == replica.node_id and pm.status == "up":
                            source_node_list.append(pm)

        source = theMinLinkLoad(source_node_list, Link_max_load, router_copy)
        # 找最小的那个链路，找到以后标记为down，下次不再考虑这条链路
        if not source:
            print("no source pm")
        else:
            flag = False
            for switch in router_copy.children:
                for pm in switch.children:
                    if pm.id == source.id:
                        source_switch = switch
                        flag = True
                        break
                if flag:
                    break

        if isHAR(data_list, data_id, router):  # 是否可以本机架替换
            print("The same rack")
            dest_node_list = list()

            for pm in source_switch.children:
                if pm.id == source.id:  # 不能同一PM
                    continue
                if pm.status == "up":
                    dest_node_list.append(pm)

            while True:
                destination = theMinLinkLoad(dest_node_list, Link_max_load, router_copy)
                if not destination:  # 本机架全部不满足替换
                    print("all pms can't satisfied replace ")
                    break
                else:
                    print("test:data_id:" + str(data_id) + " source:" + str(
                        source.id) + " destination:" + str(destination.id))
                    if pm_has_place(destination):
                        destination.slot = destination.slot - 1
                        upload_link(source, destination, Link_max_load)
                        destination_pm = copy_to_original(router, destination)

                        print("recover_rep:" + str(
                            recover_rep.id) + " source_pm:" + str(
                            source.id) + "destination_pm:" + str(destination_pm.id))
                        results.append(pack(recover_rep,source, destination_replica,
                                            destination_pm, replicas))
                        recover_success_flag = True
                        break  # 已经分配结束
                    else:
                        replica_flag = False
                        for replica in replicas:
                            if replica.node_id == destination.id:
                                if isHAR_exclude_chosen_node(data_list, replica.data_id, router_copy, destination.id):
                                    upload_link(source, destination, Link_max_load)
                                    destination_pm = copy_to_original(router, destination)
                                    print("recover_rep" + str(
                                        recover_rep.id) + "source_pm" + str(
                                        source.id) + "destination_pm:" + str(
                                        destination_pm.id))
                                    destination_replica = replica
                                    results.append(
                                        pack(recover_rep,source, destination_replica,
                                             destination_pm, replicas))
                                    replica_flag = True
                                    break  # 已经分配结束

                        if replica_flag:
                            recover_success_flag = True
                            break

        if recover_success_flag:
            print("begin the next replica")
            continue

        # ***
        print("The different rack")
        reset_pm(down_node, router_copy)
        dest_switch_list = list()
        for switch in router_copy.children:
            if source_switch is not None and source_switch.id == switch.id:
                continue
            else:
                dest_switch_list.append(switch)
        # print("dest_switch_list_length:"+str(dest_switch_list.__len__()))
        while True:
            destSwitch = theMinLinkLoadSwitch(dest_switch_list, Link_max_load,
                                              router_copy)
            dest_node_list = list()
            if destSwitch == False:
                print("data_id:" + str(data_id) + " no switch to chose")
                break
            else:
                for pm in destSwitch.children:
                    dest_node_list.append(pm)
                while True:

                    destination = theMinLinkLoad(dest_node_list, Link_max_load,
                                                 router_copy)
                    if (destination == False):
                        print(
                            "while2:data_id" + str(data_id) + "no pm to decide")
                        break

                    # print("data_id:"+str(data_id)+"source_replica"+str(source_replica.id)+"source_pm"+str(source.id)+"destination_pm:"+str(destination_pm.id))
                    print("test2:data_id:" + str(data_id) + " source:" + str(
                        source.id) + " destination:" + str(destination.id))

                    if pm_has_place(destination):
                        destination.slot = destination.slot - 1
                        upload_link(source, destination, Link_max_load)
                        destination_pm = copy_to_original(router, destination)

                        print("recover_rep:" + str(
                            recover_rep.id) + " source_pm:" + str(
                            source.id) + "destination_pm:" + str(
                            destination_pm.id))
                        results.append(pack(recover_rep, source,destination_replica,
                                            destination_pm, replicas))
                        recover_success_flag = True
                        break  # 已经分配结束

                    else:
                        replica_flag = False
                        for replica in replicas:
                            if replica.node_id == destination.id:
                                if isHAR_exclude_chosen_node(data_list, replica.data_id, router_copy, destination.id):
                                    upload_link(source, destination,
                                                Link_max_load)
                                    destination_pm = copy_to_original(router, destination)
                                    print("recover_rep" + str(
                                        recover_rep.id) + "source_pm" + str(
                                        source.id) + "destination_pm:" + str(
                                        destination_pm.id))
                                    destination_replica = replica
                                    results.append(pack(recover_rep,
                                                        source,
                                                        destination_replica,
                                                        destination_pm,
                                                        replicas))
                                    replica_flag = True
                                    break  # 已经分配结束

                        if replica_flag:
                            recover_success_flag = True
                            break
                    if recover_success_flag:
                        break
                if recover_success_flag:
                    print("next replica")
                    break
    return results


def decision_RAR(recover_list, down_node, replicas, router, Link_max_load, data_list):
    # deep copy
    router_copy = copy.deepcopy(router)
    results = list()

    # pm_id = down_node.id
    # recover_list = list()
    #
    # for replica in replicas:
    #     if pm_id == replica.node_id:  # need to recovery
    #         recover_list.append(replica)

    print("number of data: %d" % len(recover_list))

    for recover_rep in recover_list:
        recover_success_flag = False

        source_switch = None
        source_replica = recover_rep
        destination_replica = None
        reset_pm(down_node, router_copy)
        data_id = recover_rep.data_id
        source_node_list = list()
        for replica in replicas:
            if replica.status == "active" and replica.data_id == data_id:
                # find the source
                for switch in router_copy.children:
                    for pm in switch.children:
                        if pm.id == replica.node_id and pm.status == "up":
                            source_node_list.append(pm)
        source = theMinLinkLoad(source_node_list, Link_max_load, router_copy)
        # 找最小的那个链路
        if not source:
            print("no source pm")
        else:
            flag = False
            for switch in router_copy.children:
                for pm in switch.children:
                    if pm.id == source.id:
                        source_switch = switch
                        flag = True
                        break
                if flag:
                    break
        # 是否可以本机架替换
        if isHAR(data_list, data_id, router):
            print("The same rack")
            dest_node_list = list()
            for pm in source_switch.children:
                if pm.id == source.id:  # 不能同一PM
                    continue
                if pm.status == "up":
                    dest_node_list.append(pm)

            while True:
                destination = theMinLinkLoad(dest_node_list, Link_max_load,
                                             router_copy)
                if not destination:  # 本机架全部不满足替换
                    print("data_id:" + str(
                        data_id) + " all pms can't satisfied replace ")
                    break
                # print("test:data_id" + str(data_id) + "source:" + str(
                #     source.id) + "destination:" + str(destination.id))
                # print("in there")
                if pm_has_place(destination):
                    destination.slot = destination.slot - 1
                    upload_link(source, destination, Link_max_load)
                    destination_pm = copy_to_original(router, destination)

                    print("recover_rep" + str(
                        recover_rep.id) + "source_pm" + str(
                        source.id) + "destination_pm:" + str(destination_pm.id))
                    results.append(pack(recover_rep, source,destination_replica,
                                        destination_pm, replicas))
                    recover_success_flag = True
                    break  # 已经分配结束
                else:
                    replica_flag = False
                    for replica in replicas:
                        if replica.node_id == destination.id:
                            if (isHAR_exclude_chosen_node(data_list, replica.data_id, router_copy, destination.id)):
                                # 计算稀有度
                                replica_back = copy.deepcopy(replica)
                                Data = find_data(data_list, replica)
                                Data.update_rarity()
                                Data.del_rep(replica)
                                Data.update_rarity()
                                if Data.rarity > 0:
                                    upload_link(source, destination,
                                                Link_max_load)
                                    destination_pm = copy_to_original(router,
                                                                      destination)
                                    print("recover_rep" + str(
                                        recover_rep.id) + "source_pm" + str(
                                        source.id) + "destination_pm:" + str(
                                        destination_pm.id))
                                    destination_replica = replica
                                    results.append(pack(source_replica,
                                                        source,
                                                        destination_replica,
                                                        destination_pm,
                                                        replicas))
                                    replica_flag = True
                                    break  # 已经分配结束
                                else:
                                    Data.add_rep(replica_back)
                                    Data.update_rarity()
                    if replica_flag:
                        recover_success_flag = True
                        break
        if recover_success_flag:
            print("begin the next replica")
            continue

        # 下面开始另一个机柜上
        print("The different rack")
        reset_pm(down_node, router_copy)
        dest_switch_list = list()
        for switch in router_copy.children:
            if source_switch is not None and source_switch.id == switch.id:
                continue
            else:
                dest_switch_list.append(switch)
        # print("dest_switch_list_length:"+str(dest_switch_list.__len__()))
        while True:
            destSwitch = theMinLinkLoadSwitch(dest_switch_list, Link_max_load,
                                              router_copy)
            dest_node_list = list()
            if destSwitch == False:
                print("data_id:" + str(data_id) + " no switch to chose")
                break
            else:
                for pm in destSwitch.children:
                    dest_node_list.append(pm)
                while True:

                    destination = theMinLinkLoad(dest_node_list, Link_max_load,
                                                 router_copy)
                    if (destination == False):
                        print(
                            "while2:data_id" + str(data_id) + "no pm to decide")
                        break

                    # print("data_id:"+str(data_id)+"source_replica"+str(source_replica.id)+"source_pm"+str(source.id)+"destination_pm:"+str(destination_pm.id))
                    print("test2:data_id:" + str(data_id) + " source:" + str(
                        source.id) + " destination:" + str(destination.id))

                    if pm_has_place(destination):
                        destination.slot = destination.slot - 1
                        upload_link(source, destination, Link_max_load)
                        destination_pm = copy_to_original(router, destination)

                        print("recover_rep:" + str(
                            recover_rep.id) + " source_pm:" + str(
                            source.id) + "destination_pm:" + str(
                            destination_pm.id))
                        results.append(pack(recover_rep, source,destination_replica,
                                            destination_pm, replicas))
                        recover_success_flag = True
                        break  # 已经分配结束

                    else:
                        replica_flag = False
                        for replica in replicas:
                            if replica.node_id == destination.id:
                                if (isHAR_exclude_chosen_node(data_list, replica.data_id, router_copy, destination.id)):
                                    replica_back = copy.deepcopy(replica)
                                    Data = find_data(data_list, replica)
                                    Data.update_rarity()
                                    Data.del_rep(replica)
                                    Data.update_rarity()
                                    if Data.rarity > 0:
                                        upload_link(source, destination,
                                                    Link_max_load)
                                        destination_pm = copy_to_original(
                                            router, destination)
                                        print("recover_rep" + str(
                                            recover_rep.id) + "source_pm" + str(
                                            source.id) + "destination_pm:" + str(
                                            destination_pm.id))
                                        destination_replica = replica
                                        results.append(pack(recover_rep,
                                                            source,
                                                            destination_replica,
                                                            destination_pm,
                                                            replicas))
                                        replica_flag = True
                                        break  # 已经分配结束
                                    else:
                                        Data.add_rep(replica_back)
                                        Data.update_rarity()
                        if replica_flag == True:
                            recover_success_flag = True
                            break
                    if recover_success_flag:
                        break
                if recover_success_flag:
                    print("next replica")
                    break
    return results


def find_data(data_list, replica):
    for data in data_list:
        if data.id == replica.data_id:
            return data


def isHAR_exclude_chosen_node(data_list, data_id, router, node_id):
    """
    HAR算法的另一种形式，除了在node_id，同样也满足HAR
    :param data_list:
    :param data_id:
    :param router:
    :param node_id:需要排除的节点
    :return:
    """

    exist_switch = set()
    for data in data_list:
        if data.id == data_id:
            for rep in data.rep_set:
                for switch in router.children:
                    for pm in switch.children:
                        if pm.id == rep.node_id and pm.status == "up" and pm.id != node_id:
                            exist_switch.add(pm.parent)
                            break

    if len(exist_switch) > 1:  # 该数据副本至少位于其他2个机架上
        # print('HAR True')
        return True
    else:
        # print("HAR False")
        return False


def theMinLinkLoad(node_list, Link_max_load, router_copy):
    """
    最底层链路的最小链路，同时，标记该pm的node为down，下次不再选
    :param node_list:
    :param Link_max_load:
    :param router_copy:
    :return:
    """
    PreLink = Link_max_load * Link_max_load
    index = -1
    i = 0
    # find the minimum
    for pm in node_list:
        flag = False

        for switch in router_copy.children:
            for pm_copy in switch.children:
                if pm_copy.id == pm.id and pm_copy.status == "up":
                    if pm.link.load < PreLink:
                        PreLink = pm.link.load
                        index = i
                        flag = True
                        break
            if flag:
                break
        i = i + 1
    if index != -1:
        source = node_list[index]
        for switch in router_copy.children:
            for pm in switch.children:
                if pm.id == source.id:
                    pm.status_down()  # 标记down下次不能再选该pm
        return source


def theMinLinkLoadSwitch(node_list, Link_max_load, router_copy):
    """
    寻找交换机里面最链路最小的，同时，标记该交换机的node为down，下次不再选
    :param node_list:
    :param Link_max_load:
    :param router_copy:
    :return:
    """

    PreLink = Link_max_load * Link_max_load
    index = -1
    i = 0
    for pm in node_list:
        flag = False
        #        print("switch_id:"+str(pm.id))
        for switch in router_copy.children:
            #           print("switch_id:"+str(switch.id))
            if switch.id == pm.id and switch.status == "up":
                if pm.link.load < PreLink:
                    PreLink = pm.link.load
                    index = i
        i = i + 1
    print(index)
    if index != -1:
        source = node_list[index]
        for switch in router_copy.children:
            if switch.id == source.id:
                switch.status_down()  # 标记down下次不能再选该switch
        return source
    else:
        return False


def pm_has_place(destination):
    """
    pm的储存空间是否够
    :param destination:
    :return:
    """

    if destination.slot > 0:
        return True
    else:
        return False


def upload_link(source, destination, Link_max_load):
    """
    链路的值进行更新
    :param source:
    :param destination:
    :param Link_max_load:
    :return:
    """

    source.link.load += Link_max_load
    destination.link.load += Link_max_load
    if source.parent.id != destination.parent.id:
        source.parent.link.load += Link_max_load
        destination.parent.link.load += Link_max_load


def reset_pm(down_node, router_copy):
    """
    恢复pm成最初始，一个down 其他全都up，这里用down来标记该PM不满足替换
    :param down_node: 出现故障的主机
    :param router_copy:整个网络
    :return:
    """

    for switch in router_copy.children:
        switch.status_up()
        for pm in switch.children:
            if down_node.id == pm.id:
                pm.status_down()
            else:
                pm.status_up()


def copy_to_original(router, destination):
    """
    从copy的副本到源数据的转换
    :param router: 源
    :param destination: 在副本里面的目的pm
    :return:
    """
    for switch in router.children:
        for pm in switch.children:
            if pm.id == destination.id:
                return pm


def pack(dead_replica, source,destination_replica, destination_pm, replicas):
    """
    将这三个参数打包成per_result，返回
    :param source_replica:源副本
    :param destination_replica:目的副本
    :param destination_pm:目的物理机
    :param replicas:
    :return:
    """
    source_replica = None
    per_result = dict()
    for replica in replicas:
        # 找源副本
        if replica.node_id == source.id and replica.data_id == dead_replica.data_id:
            source_replica = replica
    per_result["source_replica"] = source_replica
    per_result["replace_replica"] = destination_replica
    per_result["target_pm"] = destination_pm
    print("===========================================")
    print(source_replica)
    print(destination_replica)
    print(destination_pm)
    print("===========================================")
    return per_result
