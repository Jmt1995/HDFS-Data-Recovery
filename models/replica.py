class Replica:
    def __init__(self, replica_id, data_id, node_id, load=None):
        self.id = replica_id
        self.data_id = data_id
        self.node_id = node_id
        self.load = load
        self.status = "active"

    def __str__(self):
        return "Replica id = " + str(self.id) + " , data id = " + str(
            self.data_id) + " , node_id = " + str(
            self.node_id) + " , load = %.1f" % self.load \
               + ", status = " + self.status + " | id=%d" % id(self)

    def get_replica_load(self):
        return self.load

    def status_dead(self):
        self.status = "dead"
        return self

    def status_active(self):
        self.status = "active"
        return self

    def status_migrated(self):
        self.status = "migrated"
        return self

    def status_replaced(self):
        self.status = "replaced"
        return self
