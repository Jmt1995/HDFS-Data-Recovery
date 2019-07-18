class Task:
    def __init__(self, task_id, source_replica, start_time, target_pm=None,
                 replace_replica=None):
        self.id = task_id
        self.source_replica = source_replica
        self.replace_replica = replace_replica
        self.target_pm = target_pm
        self.start_time = start_time
        self.end_time = -1
        self.load = -1
        self.process = 0
        self.status = "active"

    def __str__(self):
        ret = "Task id = %d, source replica id = %d" % (
            self.id, self.source_replica.id)
        if self.replace_replica:
            ret += ", replace replica id = %d" % self.replace_replica.id
        if self.target_pm:
            ret += ", target PM id = %d" % self.target_pm.id
        ret += ", start time = %.1f, end time = %.1f" \
               ", load = %.1f, process = %.1f, status = %s" \
               % (self.start_time, self.end_time,
                  self.load, self.process, self.status)
        ret += ", id=%d" % id(self)
        return ret

    def update_process(self, min_eta, D_size):
        if self.process == -1 and self.load != -1:
            # 乘时间
            self.process = (self.load / 8) * min_eta / D_size
        else:
            self.process = ((self.load / 8) * min_eta + (self.process * D_size)) / D_size

        # if self.process > 1:
            # print("|||", end="")
            # print(self)
            # self.process = 1
        return self

    def ETA(self, D_size):
        if self.process > 1:
            self.process = 1
        if self.process >= 0 and self.load >= 0:
            eta = ((1 - self.process) * D_size) / (self.load / 8)
        return eta

    def get_total_time(self):
        return self.end_time - self.start_time

    def status_done(self):
        self.status = "done"
        return self

    def status_active(self):
        self.status = "active"
        return self
