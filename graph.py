import matplotlib.pyplot as plt
import numpy as np
import json
from collections import OrderedDict
u_axis = list()
ddr_time_axis = list()
sdr_time_axis = list()
radr_time_axis = list()
rasdr_time_axis = list()
ddr_qos_axis = list()
sdr_qos_axis = list()
radr_qos_axis = list()
rasdr_qos_axis = list()

# if not data:
with open("u_t_qos_data_2") as file:
    data = eval(file.read())
    # data = json.load(data)
    # print(data['50'])
    print(data)

    for i in range(40,90,10):
        # get("DDR_time")
        # print(data[str(i)])
        # if i == 50:
        #     continue
        u_axis.append(str(i))
        ddr_time_axis.append(data[str(i)].get("DDR_time"))
        sdr_time_axis.append(data[str(i)].get("SDR_time"))
        radr_time_axis.append(data[str(i)].get("RADR_time"))
        rasdr_time_axis.append(data[str(i)].get("RASDR_time"))
        ddr_qos_axis.append(data[str(i)].get("DDR_QoS"))
        sdr_qos_axis.append(data[str(i)].get("SDR_QoS"))
        radr_qos_axis.append(data[str(i)].get("RADR_QoS"))
        rasdr_qos_axis.append(data[str(i)].get("RASDR_QoS"))
# rasdr_qos_axis = [80, 60, 75, 20, 10]
# rasdr_qos_axis = [80]
# rasdr_time_axis = [1000.832, 1200.0320000000004, 2400.4800000000005, 2600.1679999999997, 2400.096]
# rasdr_time_axis = [1000.832]

    # for u, value in data.items():
    #     if 40 <= int(u) < 90:
    #         u_axis.append(u)
    #         ddr_time_axis.append(value.get("DDR_time"))
    #         sdr_time_axis.append(value.get("SDR_time"))
    #         radr_time_axis.append(value.get("RADR_time"))
    #         ddr_qos_axis.append(value.get("DDR_QoS"))
    #         sdr_qos_axis.append(value.get("SDR_QoS"))
    #         radr_qos_axis.append(value.get("RADR_QoS"))
# print("u_axis",u_axis)
# print(ddr_time_axis)
# print("rasdr_time_axis", rasdr_time_axis)
#图1
plt.figure()
plt.ylim([1000, 3500])
plt.xlabel("u / %", fontsize=14)
plt.ylabel("t / s", fontsize=14)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.plot(u_axis, ddr_time_axis, color="green", marker=".", linestyle=":", label="DDR")
plt.plot(u_axis, sdr_time_axis, color="red", marker="o", linestyle="-.", label="SDR")
plt.plot(u_axis, radr_time_axis, color="magenta", marker="^", linestyle="--", label="RADR")
plt.plot(u_axis, rasdr_time_axis, color="blue", marker="s", linestyle="-", label="RASDR")
plt.legend(loc='upper left')
plt.show()
#图2
plt.figure()
plt.xlabel("u / %", fontsize=14)
plt.ylabel("QoS / %", fontsize=14)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.plot(u_axis, ddr_qos_axis, color="green", marker=".", linestyle=":", label="DDR")
plt.plot(u_axis, sdr_qos_axis, color="red", marker="o", linestyle="-.", label="SDR")
plt.plot(u_axis, radr_qos_axis, color="magenta", marker="^", linestyle="--", label="RADR")
print("rasdr_qos_axis", rasdr_qos_axis)
plt.plot(u_axis, rasdr_qos_axis, color="blue", marker="s", linestyle="-", label="RASDR")
plt.legend(loc='upper right')
plt.show()

service_time_ddr = list()
service_time_sdr = list()
service_time_radr = list()
service_qos_ddr = list()
service_qos_sdr = list()
service_qos_radr = list()

# har-t图3
t_ddr = list()
t_sdr = list()
t_radr = list()
t_rasdr = list()

har_ddr = list()
har_sdr = list()
har_radr = list()
har_rasdr = list()
# har_rasdr = [0.799, 0.999, 0.999, 0.999, 0.999, 0.999, 0.999, 0.999, 0.999]
# t_rasdr = [1000.3360000000002, 1500.0, 1600.432, 2500.3280000000004, 2515.808, 2541.5200000000004, 2516.9440000000004, 2591.0, 2600.1360000000004]

with open('t_har_8') as file:
    # data = eval(file.read())
    data = json.load(file,object_pairs_hook=OrderedDict)

    for t, har in data.get("DDR").items():
        t_ddr.append(float(t))
        har_ddr.append(har)

    for t, har in data.get("SDR").items():
        t_sdr.append(float(t))
        har_sdr.append(har)

    for t, har in data.get("RADR").items():
        t_radr.append(float(t))
        har_radr.append(har)

    for t, har in data.get("RASDR").items():
        t_rasdr.append(float(t))
        har_rasdr.append(har)

# print(len(t_sdr))
# print(len(har_sdr))
# print(data)
plt.figure()
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.xlabel("t / s", fontsize=14)
plt.ylabel("β", fontsize=14)
plt.step(t_ddr, har_ddr, color="green", marker="s", linestyle=":", label="DDR", where="post")
plt.step(t_sdr, har_sdr, color="red", marker="^", linestyle="-.", label="SDR", where="post")
plt.step(t_radr, har_radr, color="magenta", marker="o", linestyle="--", label="RADR", where="post")
plt.step(t_rasdr, har_rasdr, color="blue", marker="s", linestyle="-", label="RASDR", where="post")
# print(har_radr)
# print(t_radr)
plt.legend(loc="lower right")
plt.show()

# plt.plot(u_axis, ddr_qos_axis, color="green", marker="s", linestyle=":", label="DDR")
# plt.plot(u_axis, sdr_qos_axis, color="red", marker="^", linestyle="-.", label="SDR")
# plt.plot(u_axis, radr_qos_axis, color="magenta", marker="o", linestyle="--", label="RADR")
# plt.plot(u_axis, rasdr_qos_axis, color="blue", marker="s", linestyle="-", label="RASDR")