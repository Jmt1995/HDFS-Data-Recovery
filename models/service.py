from collections import OrderedDict


class Service:
    def __init__(self, service_id, replica_id, desire_load, load, QoS=None, parent_service=None):
        self.id = service_id
        self.replica_id = replica_id
        self.load = load  # 实际负载值
        self.desire_load = desire_load  # 期望负载值
        self.status = "active"
        self.parent_service = parent_service
        if not QoS:
            self.QoS = OrderedDict()
        else:
            self.QoS = QoS

    def __str__(self):
        ret = "Service id = " + str(self.id) + ", replica id = " + str(
            self.replica_id) + ", load = %.1f" % self.load \
               + ", desire load = %d" % self.desire_load \
               + ", status = " + self.status
        if self.parent_service:
            ret += ", parent service = %d" % self.parent_service.id

        return ret

    def get_service_load(self):
        return self.load

    def get_service_qos(self):
        return self.QoS

    def status_migrated(self):
        self.status = "migrated"
        return self

    def status_dead(self):
        self.status = "dead"
        return self

    def status_active(self):
        self.status = "active"
        return self
