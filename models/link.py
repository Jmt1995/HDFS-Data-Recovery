class Link:
    def __init__(self, node_1, node_2, load=-1):
        self.node_1 = node_1
        self.node_2 = node_2
        self.load = load
        self.services = list()
        self.tasks = list()

    def get_link_load(self):
        return self.load

    def add_service(self, service):
        self.services.append(service)
        return self

    def del_service(self, service):
        self.services.remove(service)
        return self

    def add_task(self, task):
        self.tasks.append(task)
        return self

    def del_task(self, task):
        self.tasks.remove(task)
        return self
