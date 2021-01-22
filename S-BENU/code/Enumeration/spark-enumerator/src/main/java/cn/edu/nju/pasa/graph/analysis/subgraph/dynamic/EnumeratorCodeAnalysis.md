# Enumerator Code Analysis

Class: `cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.Enumerator`.

## Enumerator成员变量组成

Enumerator的成员变量有两类组成：只与执行计划相关、与执行过程相关。其中与执行计划相关的成员变量由执行计划唯一确定，在任务执行过程中是只读的。而与执行过程相关的成员变量，会根据执行过程中变量取值的不同动态确定，这些变量在任务执行过程中会动态改变，必须作为现场保存。

整个Enumerator类中只有`executeVertexTask`方法触发实际的子图枚举，因此在该方法中被修改的成员变量均为__与执行过程相关__的变量。

Table: 与执行过程相关的变量列表

| 变量 | 含义 |
|------|------|
| boolean deleted | 记录当前的op是否为'-' |
| IntArrayList[] result | 保存各指令目标变量的结果 |
| int i | 当前正在执行的指令编号 i |
| MatchesCollectorInterface collector | 收集匹配结果的类 |


