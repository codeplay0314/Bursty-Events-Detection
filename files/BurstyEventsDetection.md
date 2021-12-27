
## Bursty Event Detetion
此版本实现主要参照**VLDB‘05**论文*Parameter Free Bursty Events Detection in Text Streams*，主要结构和算法保持不变，但因从批处理改成流处理，仍有部分实现有修改（如用cache一定时间段的数据代替历史数据）。  
代码仓库地址：https://github.com/codeplay0314/Bursty-Events-Detection/tree/main/src/storm/src/main/java/BurstyEventsDetection

### 代码结构

```cmd
BurstyEventsDetection
│   BurstyEventsBolt.java
│   BurstyEventsDetectionTopology.java
│   BurstyFeaturesBolt.java
│   DataCollectBolt.java
│   FeatureProcessBolt.java
│   HotPeriodBolt.java
│   NewsSpout.java
│
├───lib
│       Binomial.java
│       BurstyProb.java
│       Calc.java
│       UnionFind.java
│
└───module
        Document.java
        Event.java
        Feature.java
        FeatureInfo.java
```
#### Storm Topology
如下，Storm Topology的结构定义在`BurstyEventsDetectionTopology.java`，共有1个Spout和5个Bolts.  
![Topology](https://img-blog.csdnimg.cn/43f09826d7374609934e4bbfea425a64.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBAQ29kZXBsYXkwMzE0,size_20,color_FFFFFF,t_70,g_se,x_16#pic_center)
[`NewsSpout.java`] 模拟了数据的流输入，从文件读入并定时打包发送时间戳相同的数据  
[`BurstyFeaturesBolt.java`] 将每天的feature的文档信息写成一个内部静态类`FeatureInfo.Info`，并按fieldsGrouping发送到下一个bolt  
[`FeatureProcessBolt.java`] cache了`expire_day`天的数据，同一feature合并成一个`FeatureInfo`打包发送下游  
[`DataCollect.java` ]将上一步并行计算的`FeatureInfo`进行打包（进行了同步化操作）
[`BurstyEventsBolt.java`] 将整天的features进行聚类，设数量为n，原文的方法复杂度为O(2<sup>n</sup> * n)，这里修改为贪心连边，复杂度降为O(n<sup>3</sup>)。同时，在这里进行了feature的筛选，滤掉了出现平均频率过高（Stopwords）和过低（非Bursty）。向下游发送Event集合  
[`HotPeriodBolt.java`] 跟拒Event和Doc数据计算Event在当下突发的概率并输出结果  
```java
builder.setSpout("News", new NewsSpout(), 1);
builder.setBolt("BurstyFeatures", new BurstyFeaturesBolt(), 5)
        .shuffleGrouping("News");
builder.setBolt("FeatureProcess", new FeatureProcessBolt(), 10)
        .fieldsGrouping("BurstyFeatures", new Fields("feature"));
builder.setBolt("DataCollect", new DataCollectBolt(), 1)
        .globalGrouping("BurstyFeatures", "FeatureCount")
        .globalGrouping("FeatureProcess");
builder.setBolt("BurstyEvents", new BurstyEventsBolt(), 1)
        .globalGrouping("DataCollect");
builder.setBolt("HotPeriod", new HotPeriodBolt(), 1)
        .globalGrouping("BurstyEvents")
        .globalGrouping("DataCollect");
```

除了以上主要部分，还包含一下依赖文件  
#### lib
[`Binomial.java`] 计算二项分布概率  
[`BurstyProb.java`] 计算feature突发概率
[`Calc.java`] 计算数组平均值和方差  
[`UnionFind.java`] 并查集

#### module
[`Document.java`, `Event.java`, `Feature.java`,`FeatureInfo.java`] 定义了结构中用到的四个类

### 实现的问题

#### 冷启动
据观察，原文给的算法在数据量极小的时候会将所有feature都判为bursty，即冷启动问题，这里的解决方案是丢弃前7天的运行结果。
#### 并行问题
由于原文给出算法均基于批处理，改为流处理的实现方法中亦无太多可并行计算之处，仅在`FeatureProcess`一处根据feature字段进行并行处理，但造成了之后数据同步问题，额外增加了处理成本，故此计算逻辑不适合Storm/Flink这类的流框架。
#### 内存问题
原文算法需要实时依靠历史信息，在流计算中显然是无法做到不断增加存储的，故在各个bolts实现了cache一定期限的信息的功能，使能访问到一定时间段的历史信息。
#### 数据合规性检查
这版实现没有进行数据合规性检查，输入错误格式数据可能会造成多个板块崩溃，只能靠重启解决，这样又会带来一次冷启动。