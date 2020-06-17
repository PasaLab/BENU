GraphEnumerationOnDataParallelPlatform
===============

Graph enumeration on data parallel platform.

## Change log

### Version4

1. 为VertexTask增加随机盐值，使task优先按盐值，打乱顶点的顺序，使各个任务的负载在Reduce阶段时间上分布均匀。
2. 增加配置store.graph.to.db，用以指定是否在map阶段将图数据写入数据库。
   如果为true，则强制写入数据库；如果为false，则会检查当前数据库中保存的数据图文件路径（以-100为key保存）是否和用户输入的图文件路径hash值相同。
   如果相同，则不在map阶段写数据库，如果不同，则会在map阶段写数据库。
   在Driver执行的最后，会将当前的图文件所在路径写入数据库。
3. 在CommonsFunction中增加Adaptive版本的集合求交集的运算。在Adaptive版本中，有两种实现集合求交的方式：merge, binary search。会先估计两个开销的大小，选择代价小的。