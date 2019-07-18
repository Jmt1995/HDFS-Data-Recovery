from collections import OrderedDict
time_har_rasdr = dict()
time_har_rasdr["A"]=1
time_har_rasdr["b"]=2
time_har_rasdr["c"]=3

time_har_rasdr = OrderedDict(sorted(time_har_rasdr.items(), key=lambda t: t[0]))
# print(OrderedDict(sorted(time_har_rasdr.items(), key=lambda t: t[0])))
print(time_har_rasdr)