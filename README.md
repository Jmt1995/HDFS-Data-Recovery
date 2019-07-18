# HDFS-Data-Recovery
@author Triple Z
Experiment.

## Config

- `M_cap`: 每台机器的存储量，单位为 `TiB`；
- `N_rack`: 机架的个数；
- `N_r_pm`: 每个机架中服务器的数目；
- `D_size`: 单一数据块大小，单位为 `MiB`；
- `Link_max_load`: 最大链路带宽，单位为 `Mbps`；
- `Rep_set_range`: 初始副本数目范围；
- `Target_rep_num`: 目标副本数；
- `Service_load_range`: 初始服务带宽需求范围；
- `u`: 上层链路平均利用率，单位 `%`；
- `beta`: 稀有度公式中的变化参数;
