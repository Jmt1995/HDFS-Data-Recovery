import matplotlib.pyplot as plt

rasdr_time_axis = list()
rasdr_qos_axis = list()

rasdr_qos_axis = [80, 60, 75, 20, 10]
rasdr_time_axis = [1000.832, 1200.0320000000004, 2400.4800000000005, 2600.1679999999997, 2400.096]



plt.figure()
plt.ylim([1000, 3500])
plt.xlabel("u / %", fontsize=14)
plt.ylabel("t / s", fontsize=14)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)

plt.plot(rasdr_qos_axis, rasdr_time_axis, color="blue", marker="s", linestyle="-", label="RASDR")
plt.legend(loc='upper left')
plt.show()
