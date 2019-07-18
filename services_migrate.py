from models.service import Service
from update import *


def migrate_dead_services(down_node, router, data_list, replicas, services,
                          Link_max_load):
    pm_id = down_node.id

    services_in_down_node = dict()

    for replica in replicas:
        if pm_id == replica.node_id:
            replica_id = replica.id
            for service in services:
                if replica_id == service.replica_id:
                    services_in_down_node[service] = replica

    for service, replica in services_in_down_node.items():
        replicas_belongs_to_data = list()
        data_id = replica.data_id
        for data in data_list:
            if data.id == data_id:
                # 找该数据的其他活跃副本
                for _replica in replicas:
                    if _replica.data_id == data_id \
                            and _replica.status == "active":
                        replicas_belongs_to_data.append(_replica)

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

    update_network_load(router, Link_max_load)
    update_services_qos(services)

    return services
