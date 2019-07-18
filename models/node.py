from models.link import Link


class Node:
    def __init__(self, node_id, slot=None):
        self.id = node_id
        self.parent = None
        self.children = list()
        self.slot = slot
        self.link = None
        self.status = "up"

    def __str__(self):
        rtn = ' - ' + str(self.id) + " | status: " + self.status

        if self.slot:
            rtn += " | available slot = " + str(self.slot)

        if self.link is not None:
            rtn += " | link load = %.1f" % self.link.load
            rtn += " | services: "
            for service in self.link.services:
                rtn += str(service.id) + ", "
            rtn += " | tasks: "
            for task in self.link.tasks:
                rtn += str(task.id) + ", "

        return rtn

    def add_child(self, new_child_id, slot=None):
        new_child = Node(new_child_id, slot)
        new_child.parent = self
        new_child.link = Link(self, new_child, 0)
        self.children.append(new_child)

        return new_child

    def del_child(self, del_child_id):
        for child in self.children:
            if child.id == del_child_id:
                self.children.remove(child)
                del child

        return True

    def status_down(self):
        self.status = "down"
        return self

    def status_up(self):
        self.status = "up"
        return self

    def print_tree(self, depth=0):
        for i in range(0, depth):
            print('\t', end="")

        case = {
            0: "Router",
            1: "Switch",
            2: "PM",
        }.get(depth)

        print(' - ' + str(self.id) + " | " + case + " | status: " + self.status
              , end="")
        # print(" | id = %d" % id(self), end="")

        if self.slot is None:
            print("", end="")
        else:
            print(" | available slot = " + str(self.slot), end="")

        if self.link is not None:
            print(" | link load = %.1f" % self.link.load, end="")
            print(" | services: ", end="")
            for service in self.link.services:
                print(str(service.id) + ", ", end="")
            print(" | tasks: ", end="")
            for task in self.link.tasks:
                print(str(task.id) + ", ", end="")


        print("")

        depth += 1
        for child in self.children:
            child.print_tree(depth)

        return True

    def add_slot(self, max_slot, num=1):
        self.slot += num
        if self.slot > max_slot:
            self.slot = max_slot
            return False
        return self

    def del_slot(self, num=1):
        if self.slot - num >= 0:
            self.slot -= num
            return self
        else:
            print("There is no more slot to apply in PM %d!" % self.id)
            return False
