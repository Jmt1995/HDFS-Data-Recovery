from models.node import Node
from models.data import Data
from models.link import Link
from models.replica import Replica
from models.service import Service
import json
from collections import OrderedDict
from json.decoder import JSONDecodeError
from init import *
from incident import *
import global_vars as gl
from utils import *
from decision import *
from services_migrate import *
from replicas_migrate import *
import copy
from evaluation import *
import matplotlib.pyplot as plt


def main():
    """
    Main function
    """

    '''
    Read the config file
    '''

    with open('config.json') as config_file:
        try:
            config_data = json.load(config_file)
            print(config_data)
        except JSONDecodeError:
            print("[ERROR] Invalid json format in config file!")
            exit(1)

    M_cap = config_data.get("M_cap")  # Unit: TiB
    N_rack = config_data.get("N_rack")
    N_r_pm = config_data.get("N_r_pm")
    D_size = config_data.get("D_size")  # Unit: MiB
    D_type = config_data.get("D_type")
    Link_max_load = config_data.get("Link_max_load")  # Unit: Mbps
    Rep_set_range = config_data.get("Rep_set_range")
    Target_rep_num = config_data.get("Target_rep_num")
    Service_load_range = config_data.get("Service_load_range")  # Unit: Mbps
    Hdfs_iteration = config_data.get("Hdfs_iteration")
    Hdfs_interval = config_data.get("Hdfs_interval")




    beta = config_data.get("beta")

    max_slot = int((M_cap * 1024 * 1024) / D_size)

    # Exp data collection
    u_data = dict()
    service_qos = dict()
    chosen_service_id = -1
    data_har = dict()

    batch = 1

    for i in range(0, batch):

        for u in range(40, 50, 10):

            '''
            Initialize
            '''

            print("--------------- Initialize ---------------", end="")
            print('------------------------------------------------------------')

            gl.time = 0

            router = init_nodes(N_rack, N_r_pm, max_slot)

            data_list = init_data(D_type)

            replicas = init_replicas(data_list, Rep_set_range, 0, router)

            services = init_services(router, replicas, data_list, Service_load_range, 0,
                                     u, Link_max_load, max_slot)

            # router.print_tree()
            # print_data(data_list)
            # print_replicas(replicas)
            # print_services(services)

            '''
            Incident Break
            '''

            print("--------------- Incident Break ---------------", end="")
            print('-----------------------------------------------------------')

            chosen_down_node_id = seek_incident_break_node_id(router)
            print("Node " + str(chosen_down_node_id) + " will be down!")
            down_node = incident_break(router, chosen_down_node_id)
            update_break_status(down_node, data_list, replicas, services)

            # router.print_tree()
            # print_data(data_list)
            # print_replicas(replicas)
            # print_services(services)

            '''
            Service Migration
            '''

            print("--------------- Service Migration ---------------", end="")
            print('-----------------------------------------------------------')
            # 增加一个时间片
            gl.time += 1

            services = migrate_dead_services(down_node, router, data_list, replicas, services, Link_max_load)

            print("============ Source Network ===========")
            router.print_tree()
            print_data(data_list)
            print_replicas(replicas)
            print_services(services)

            # 拷贝数据 - DR 组
            router_dr, data_list_dr, replicas_dr, services_dr = copy_whole_network(router, data_list, replicas, services)
            down_node_dr = get_down_node(router_dr, down_node.id)

            # print("============ DR Network ===========")
            # router_dr.print_tree()
            # print_data(data_list_dr)
            # print_replicas(replicas_dr)
            # print_services(services_dr)

            # 拷贝数据 - SR 组
            router_sr, data_list_sr, replicas_sr, services_sr = copy_whole_network(router, data_list, replicas, services)
            down_node_sr = get_down_node(router_sr, down_node.id)

            # print("============ SR Network ===========")
            # router_sr.print_tree()
            # print_data(data_list_sr)
            # print_replicas(replicas_sr)
            # print_services(services_sr)

            # 拷贝数据 - RAR 组
            router_rar, data_list_rar, replicas_rar, services_rar = copy_whole_network(router, data_list, replicas, services)
            down_node_rar = get_down_node(router_rar, down_node.id)

            # print("============ RAR Network ===========")
            # router_rar.print_tree()
            # print_data(data_list_rar)
            # print_replicas(replicas_rar)
            # print_services(services_rar)


            # 拷贝数据 - RASR 组
            router_rasr, data_list_rasr, replicas_rasr, services_rasr = copy_whole_network(router, data_list, replicas, services)
            down_node_rasr = get_down_node(router_rasr, down_node.id)

            # print("============ RASR Network ===========")
            # router_rasr.print_tree()
            # print_data(data_list_rasr)
            # print_replicas(replicas_rasr)
            # print_services(services_rasr)
            '''
            Replica Migration
            '''

            print("--------------- Replica Migration ---------------", end="")
            print('-----------------------------------------------------------')
            # 增加一个时间片
            gl.time += 1

            print("---------------------------- DDR -----------------------------")
            tasks_dr = migrate_dead_replicas("DDR", down_node_dr, router_dr, data_list_dr
                                             , replicas_dr, services_dr, Link_max_load, D_size
                                             , Target_rep_num, beta, max_slot)
            DDR_time = gl.time
            print("End time = %.2f" % gl.time)
            # print_services(services_dr)


            gl.time = 2
            print("---------------------------- SDR -----------------------------")
            tasks_sr = migrate_dead_replicas("SDR", down_node_sr, router_sr, data_list_sr
                                             , replicas_sr, services_sr, Link_max_load,
                                             D_size
                                             , Target_rep_num, beta, max_slot)
            SDR_time = gl.time
            print("End time = %.1f" % gl.time)

            gl.time = 2
            print("---------------------------- RADR -----------------------------")
            tasks_rar = migrate_dead_replicas("RADR", down_node_rar, router_rar, data_list_rar
                                              , replicas_rar, services_rar, Link_max_load,
                                              D_size
                                              , Target_rep_num, beta, max_slot)
            RADR_time = gl.time
            print("End time = %.1f" % gl.time)

            gl.time = 2
            print("---------------------------- RASDR -----------------------------")
            tasks_rasr = migrate_dead_replicas("RASDR", down_node_rasr, router_rasr, data_list_rasr
                                              , replicas_rasr, services_rasr, Link_max_load,
                                              D_size
                                              , Target_rep_num, beta, max_slot)
            RASDR_time = gl.time
            print("End time = %.1f" % gl.time)

            # HAR and t
            time_har_ddr = dict()
            for data in data_list_dr:
                for t, har in data.HAR.items():
                    if har:
                        if time_har_ddr.get(t):
                            time_har_ddr[t] += 1
                        else:
                            time_har_ddr[t] = 1
            for t, har_num in time_har_ddr.items():
                time_har_ddr[t] /= len(data_list_dr)

            time_har_ddr = OrderedDict(sorted(time_har_ddr.items(), key=lambda t: t[0]))##
            data_har["DDR"] = time_har_ddr
            print(data_har)

            time_har_sdr = dict()
            for data in data_list_sr:
                for t, har in data.HAR.items():
                    if har:
                        if time_har_sdr.get(t):
                            time_har_sdr[t] += 1
                        else:
                            time_har_sdr[t] = 1
            for t, har_num in time_har_sdr.items():
                time_har_sdr[t] /= len(data_list_sr)

            time_har_sdr = OrderedDict(sorted(time_har_sdr.items(), key=lambda t: t[0]))##
            data_har["SDR"] = time_har_sdr
            print(data_har)

            time_har_radr = dict()
            for data in data_list_rar:
                for t, har in data.HAR.items():
                    if har:
                        if time_har_radr.get(t):
                            time_har_radr[t] += 1
                        else:
                            time_har_radr[t] = 1
            for t, har_num in time_har_radr.items():
                time_har_radr[t] /= len(data_list_rar)

            time_har_radr = OrderedDict(sorted(time_har_radr.items(), key=lambda t: t[0]))##
            data_har["RADR"] = time_har_radr
            print(data_har)

            time_har_rasdr = dict()
            for data in data_list_rasr:
                for t, har in data.HAR.items():
                    if har:
                        if time_har_rasdr.get(t):
                            time_har_rasdr[t] += 1
                        else:
                            time_har_rasdr[t] = 1
            for t, har_num in time_har_rasdr.items():
                time_har_rasdr[t] /= len(data_list_rasr)



            time_har_rasdr = OrderedDict(sorted(time_har_rasdr.items(), key=lambda t: t[0]))##
            data_har["RASDR"] = time_har_rasdr
            print(data_har)




            # 提取服务
            for service in services_dr:
                if service.status == "active" and not service.parent_service:
                    chosen_service_id = service.id
                    service_qos['DDR'] = service.QoS

            for service in services_sr:
                if service.id == chosen_service_id:
                    service_qos['SDR'] = service.QoS

            for service in services_rar:
                if service.id == chosen_service_id:
                    service_qos['RADR'] = service.QoS

            for service in services_rasr:
                if service.id == chosen_service_id:
                    service_qos['RASDR'] = service.QoS

            print(service_qos)

            print("======================== DDR Tasks ========================")
            print_tasks(tasks_dr)
            # print_services(services_dr)
            # print_data(data_list_dr)
            # router_dr.print_tree()
            #
            print("======================== SDR Tasks ========================")
            print_tasks(tasks_sr)
            # print_services(services_sr)
            # print_data(data_list_sr)
            # router_sr.print_tree()
            #
            print("======================== RADR Tasks ========================")
            print_tasks(tasks_rar)
            print_data(data_list_rar)
            # print_services(services _rar)
            # router_rar.print_tree()

            # Generate u_value_data
            temp_u_value = u_data.get(u)
            if temp_u_value:
                temp_u_value_DDR_time = temp_u_value.get("DDR_time")
                if temp_u_value_DDR_time:
                    print("+++++++++ Update ++++++++++")
                    u_data.get(u)["DDR_time"] += DDR_time
                    u_data.get(u)["SDR_time"] += SDR_time
                    u_data.get(u)["RADR_time"] += RADR_time
                    u_data.get(u)["RASDR_time"] += RASDR_time
                    u_data.get(u)["DDR_QoS"] += services_aver_QoS(services_dr)
                    u_data.get(u)["SDR_QoS"] += services_aver_QoS(services_sr)
                    u_data.get(u)["RADR_QoS"] += services_aver_QoS(services_rar)
                    u_data.get(u)["RASDR_QoS"] += services_aver_QoS(services_rasr)
                    print("u = %d, i = %d" % (u, i))
                    print(u_data.get(u))
            else:
                u_value_data = dict()
                u_value_data["DDR_time"] = DDR_time
                u_value_data["SDR_time"] = SDR_time
                u_value_data["RADR_time"] = RADR_time
                u_value_data["RASDR_time"] = RASDR_time
                u_value_data["DDR_QoS"] = services_aver_QoS(services_dr)
                u_value_data["SDR_QoS"] = services_aver_QoS(services_sr)
                u_value_data["RADR_QoS"] = services_aver_QoS(services_rar)
                u_value_data["RASDR_QoS"] = services_aver_QoS(services_rasr)
                u_data[u] = u_value_data
                print("u = %d, i = %d" % (u, i))
                print(u_value_data)

            # Append u_data

            # print(u_data)

    for u in range(40, 90, 10):
        # Average
        u_data.get(u)["DDR_time"] /= batch
        u_data.get(u)["SDR_time"] /= batch
        u_data.get(u)["RADR_time"] /= batch
        u_data.get(u)["RASDR_time"] /= batch
        u_data.get(u)["DDR_QoS"] /= batch
        u_data.get(u)["SDR_QoS"] /= batch
        u_data.get(u)["RADR_QoS"] /= batch
        u_data.get(u)["RASDR_QoS"] /= batch


        print(u_data)

    with open("u_t_qos_data_2", "w") as file:
        json.dump(u_data, file)
    print(data_har)
    data_har = OrderedDict(sorted(data_har.items(), key=lambda t: t[0]))
    with open("t_har_8", "w") as file:
        json.dump(data_har, file)
    print(data_har)

    # with open("service_data_4", "w+") as file:
    #     json.dump(service_qos, file)

    # t_ddr = list()
    # t_sdr = list()
    # t_radr = list()
    # har_ddr = list()
    # har_sdr = list()
    # har_radr = list()
    #
    # with open('t_har_2') as file:
    #     # data = eval(file.read())
    #     data = json.load(file)
    #
    #     for t, har in data.get("DDR").items():
    #         t_ddr.append(float(t))
    #         har_ddr.append(har)
    #
    #     for t, har in data.get("SDR").items():
    #         t_sdr.append(float(t))
    #         har_sdr.append(har)
    #
    #     for t, har in data.get("RADR").items():
    #         t_radr.append(float(t))
    #         har_radr.append(har)
    #
    # print(len(t_sdr))
    # print(len(har_sdr))
    #
    # plt.figure()
    # plt.xticks(fontsize=14)
    # plt.yticks(fontsize=14)
    # plt.xlabel("t / s", fontsize=14)
    # plt.ylabel("ɣ", fontsize=14)
    # plt.step(t_ddr, har_ddr, color="blue", marker="s", linestyle="-", where="post",
    #          label="DDR")
    # plt.step(t_sdr, har_sdr, color="red", marker="^", linestyle="--", where="post",
    #          label="SDR")
    # plt.step(t_radr, har_radr, color="green", marker="o", linestyle="-", where="post",
    #          label="RADR")
    # plt.legend(loc="upper left")
    # plt.show()


    # service_time_ddr = list()
    # service_time_sdr = list()
    # service_time_radr = list()
    # service_qos_ddr = list()
    # service_qos_sdr = list()
    # service_qos_radr = list()
    #
    # with open('service_data_4') as file:
    #     data = eval(file.read())
        # data = json.load(file)
        # print(data.get("DDR"))
        # print(type(data))
        #
        # for t, qos in data.get("DDR").items():
        #     service_time_ddr.append(float(t))
        #     service_qos_ddr.append(qos)
        #
        # for t, qos in data.get("SDR").items():
        #     service_time_sdr.append(float(t))
        #     service_qos_sdr.append(qos)
        #
        # for t, qos in data.get("RADR").items():
        #     service_time_radr.append(float(t))
        #     service_qos_radr.append(qos)
    #
    # plt.figure()
    # plt.xticks(fontsize=14)
    # plt.yticks(fontsize=14)
    # plt.xlabel("t / s", fontsize=14)
    # plt.ylabel("QoS / %", fontsize=14)
    # plt.step(service_time_ddr, service_qos_ddr, color="blue", marker="s",
    #          linestyle="-", label="DDR", where="post")
    # plt.step(service_time_sdr, service_qos_sdr, color="red", marker="^",
    #          linestyle="--", label="SDR", where="post")
    # plt.step(service_time_radr, service_qos_radr, color="green", marker="o",
    #          linestyle="-", label="RADR", where="post")
    # plt.legend(loc="upper left")
    # plt.show()

if __name__ == '__main__':
    main()
