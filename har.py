def isHAR(data_list, data_id, router):
    exist_switch = set()
    for data in data_list:
        if data.id == data_id:
            for rep in data.rep_set:
                for switch in router.children:
                    for pm in switch.children:
                        if pm.id == rep.node_id and pm.status == "up":
                            exist_switch.add(pm.parent)
                            break

    if len(set(exist_switch)) > 1:  # 该数据副本至少位于两个机架上
        # print('HAR True')
        return True
    else:
        # print("HAR False")
        return False
