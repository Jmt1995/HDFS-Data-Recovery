def services_aver_QoS(services):
    """
    服务平均 QoS
    """

    active_services = list()
    services_total_qos = 0
    for service in services:
        if service.status == "active":
            active_services.append(service)
            service_total_qos = 0
            for t, qos in service.QoS.items():
                service_total_qos += qos
            service_aver_qos = service_total_qos / len(service.QoS)
            if service_aver_qos > 100:
                print("//// service biggg %.1f len = %d" % (service_total_qos, len(service.QoS)))
            services_total_qos += service_aver_qos

    services_aver_qos = services_total_qos / len(active_services)
    if services_aver_qos > 100:
        print("//// biggg! %.2f len = %d" % (services_total_qos, len(active_services)))

    return services_aver_qos

