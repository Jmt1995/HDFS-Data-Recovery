import math
from collections import OrderedDict


class Data:
    def __init__(self, data_id):
        self.id = data_id
        self.rep_set = list()
        self.degree = -1
        self.rarity = -1
        self.HAR = OrderedDict()

    def __str__(self):
        s = "Data id = " + str(self.id) \
            + ", len(Rep_set) = %d Degree = " % len(self.rep_set) \
            + str(self.degree) + ", rarity = %.1f" % self.rarity
        return s

    def update_rarity(self, beta=1):
        if self.degree >= 0:
            self.rarity = len(self.rep_set) - (beta * math.log10(self.degree))
            return self
        else:
            return False

    def add_rep(self, replica):
        self.rep_set.append(replica)
        return self

    def del_rep(self, replica):
        self.rep_set.remove(replica)
        return self

    def get_data_degree(self):
        return self.degree

    def get_data_rep_set(self):
        return self.rep_set
