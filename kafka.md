# kafka
Producer 使用 push 模式将消息发布到 broker，Consumer 使用 pull 模式从 broker 订阅并消费消息。
    

## Kafka的优势
- 高吞吐量、低延迟：kafka每秒可以处理几十万条消息，它的延迟最低只有几毫秒
- 可扩展性：kafka集群支持热扩展
- 持久性、可靠性：消息被持久化到本地磁盘，并且支持数据备份防止数据丢失
- 容错性：允许集群中节点故障（若副本数量为n,则允许n-1个节点故障）
- 高并发：支持数千个客户端同时读写
    

## Kafka的缺点
- 消息实时: Kafka由于是批量发送，数据并非真正的实时
- 重复消息：Kafka保证每条消息至少送达一次，虽然几率很小，但一条消息可能被送达多次。
- 消息乱序：Kafka某一个固定的Partition内部的消息是保证有序的，如果一个Topic有多个Partition，partition之间的消息送达不保证有序。
- 复 杂 性：  Kafka需要Zookeeper的支持，Topic一般需要人工创建，部署和维护比一般MQ成本更高。
    

## 应用场景
- 日志收集：一个公司可以用Kafka可以收集各种服务的log，通过kafka以统一接口服务的方式开放给各种consumer
- 消息系统：解耦生产者和消费者、缓存消息等
- 用户活动跟踪：kafka经常被用来记录web用户或者app用户的各种活动，如浏览网页、搜索、点击等活动，这些活动信息被各个服务器发布到kafka的topic中，然后消费者通过订阅这些topic来做实时的监控分析，亦可保存到数据库
- 运营指标：kafka也经常用来记录运营监控数据。包括收集各种分布式应用的数据，生产各种操作的集中反馈，比如报警和报告
- 流式处理：比如spark streaming和storm
    

## Kafka组件

- Broker 已发布的消息保存在一组服务器中，它们被称为代理（Broker）或Kafka集群。
- Producer 能够发布消息到话题的任何对象。
- Consumer 可以订阅一个或多个话题，并从Broker拉数据，从而消费这些已发布的消息。
- Topic 是特定类型的消息流。消息是字节的有效负载（Payload）。
- Partition 每个分区在本地磁盘上对应一个文件夹，分区命名规则为topic名称后接“—”连接符，之后再接分区编号，分区编号从0开始至分区总数减-1。
- LogSegment 每个分区又被划分为多个日志分段（LogSegment）组成，日志段是Kafka日志对象分片的最小单位；LogSegment算是一个逻辑概念，对应一个具体的日志文件（“.log”的数据文件）和两个索引文件（“.index”和“.timeindex”，分别表示偏移量索引文件和消息时间戳索引文件）组成。

- Offset 每个partition中都由一系列有序的、不可变的消息组成，这些消息被顺序地追加到partition中。每个消息都有一个连续的序列号称之为offset—偏移量，用于在partition内唯一标识消息（并不表示消息在磁盘上的物理位置）。
    

## Replication
- 一个partition的复制个数（replication factor）包括这个partition的leader本身。
- 所有对partition的读和写都通过leader。
- Followers通过pull获取leader上log（message和offset）
- 如果一个follower挂掉、卡住或者同步太慢，leader会把这个follower从”in sync replicas“（ISR）列表中删除。
- 当所有的”in sync replicas“的follower把一个消息写入到自己的log中时，这个消息才被认为是”committed“的。
    

## leader / follower
Kafka 通过 Zookeeper 管理集群配置，选举 leader。
    
leader处理partition所有的读写请求，followers通过leader进行数据备份。 如果leader失败了，followers中的一个会自动变成leader。
    
- leader: replica  中的一个角色， producer  和 consumer  只跟 leader  交互
- follower：replica  中的一个角色，从 leader  中复制数据
     

## 写数据流程
- producer 先从 zookeeper 的 "/brokers/.../state" 节点找到该 partition 的 leader
- producer 将消息发送给该 leader
- leader  将消息写入本地 log
- followers  从 leader pull  消息，写入本地 log  后 leader  发送 ACK
- leader  收到所有 ISR(in-sync replicas) 中的 replica  的 ACK  后向 producer  发送 ACK
    

## kafka消息的存储机制
- kafka以topic来进行消息管理，每个topic包含多个partition，每个partition对应一个逻辑log，由多个segment组成。
- 每个segment中存储多条消息，消息id由其逻辑位置决定，即从消息id可直接定位到消息的存储位置，避免id到位置的额外映射。
- 每个part在内存中对应一个index，记录每个segment中的第一条消息偏移。
- 发布者发到某个topic的消息会被均匀的分布到多个partition上（或根据用户指定的路由规则进行分布）， broker收到发布消息往对应partition的最后一个segment上添加该消息，当某个segment上的消息条数达到配置值或消息发布时间超过阈值时，segment上的消息会被flush到磁盘，只有flush到磁盘上的消息订阅者才能订阅到，segment达到一定的大小后将不会再往该segment写数据，broker会创建新的segment。
- 每一个partition是一个有序的不可改变的消息队列， 它可以持续的追加——结构化的提交日志。partitions中的每一个记录都会分配 一个有序的id，这个id叫做偏移量——每一个partition中的一个消息的唯一标识符。

   
## Kafka Log的存储解析
Partition 中的每条 Message 由 offset 来表示它在这个 partition 中的偏移量，这个 offset 不是该 Message 在 partition 数据文件中的实际存储位置，而是逻辑上一个值，它唯一确定了partition 中的一条 Message。因此，可以认为 offset 是 partition 中 Message 的 id。
    
partition中的每条 Message 包含了以下三个属性：  offset; MessageSize; data   
    
那 Kafka 是如何解决查找效率的的问题呢？

- 分段：Kafka 解决查询效率的手段之一是将数据文件分段，比如有 100 条 Message，它们的 offset 是从 0 到 99。假设将数据文件分成 5 段，第一段为 0-19，第二段为 20-39，以此类推，每段放在一个单独的数据文件里面，数据文件以该段中最小的 offset 命名。这样在查找指定 offset 的 Message 的时候，用二分查找就可以定位到该 Message 在哪个段中。

- 索引：数据文件分段使得可以在一个较小的数据文件中查找对应 offset 的 Message 了，但是这依然需要顺序扫描才能找到对应 offset 的 Message。为了进一步提高查找的效率，Kafka 为每个分段后的数据文件建立了索引文件，文件名与数据文件的名字是一样的，只是文件扩展名为.index。索引文件中包含若干个索引条目，每个条目表示数据文件中一条 Message 的索引。索引包含两个部分，分别为相对 offset 和 position。
    

## Kafka消息存储策略
- 基于时间log.retention.hours，多少小时前的删除。
- 基于大小log.retention.bytes，保留最近的多少Size数据。
    

## Kafka broker
与其它消息系统不同，Kafka broker是无状态的。这意味着消费者必须维护已消费的状态信息。这些信息由消费者自己维护，broker完全不管（有offset managerbroker管理）。
    

## Producer发送确认
request.required.acks来设置，选择是否等待消息commit（是否等待所有的”in sync replicas“都成功复制了数据）。Producer可以通过acks参数指定最少需要多少个Replica确认收到该消息才视为该消息发送成功。acks的默认值是1，即Leader收到该消息后立即告诉Producer收到该消息，此时如果在ISR中的消息复制完该消息前Leader宕机，那该条消息会丢失。
    

## Producer负载均衡
- 默认路由：producer可以自定义发送到哪个partition的路由规则。默认路由规则：hash(key)%numPartitions，如果key为null则随机选择一个partition。
- 自定义路由：如果key是一个user id，可以把同一个user的消息发送到同一个partition，这时consumer就可以从同一个partition读取同一个user的消息。
    

## Consumer Position
consumer自己控制消息的读取。
    
- 大部分消息系统由broker记录哪些消息被消费了，但Kafka不是。
- Kafka由consumer控制消息的消费，consumer甚至可以回到一个old offset的位置再次消费消息。
    

## Consumer group
- 每一个consumer实例都属于一个consumer group。
- 每一条消息只会被同一个consumer group里的一个consumer实例消费。
- 不同consumer group可以同时消费同一条消息。
- 如果consumer group中consumer数量少于partition数量，则至少有一个consumer会消费多个partition的数据。
- 如果consumer的数量与partition数量相同，则正好一个consumer消费一个partition的数据。
- 如果consumer的数量多于partition的数量时，会有部分consumer无法消费该topic下任何一条消息。
    

## Message Delivery
- 读取消息，写log，处理消息。如果处理消息失败，log已经写入，则无法再次处理失败的消息，对应”At most once“。
- 读取消息，处理消息，写log。如果消息处理成功，写log失败，则消息会被处理两次，对应”At least once“。
- 读取消息，同时处理消息并把result和log同时写入。这样保证result和log同时更新或同时失败，对应”Exactly once“。
    
Kafka默认保证at-least-once delivery，容许用户实现at-most-once语义，exactly-once的实现取决于目的存储系统，kafka提供了读取offset，实现也没有问题。
    

## Consumer Offset
- High-level consumer记录每个partition所消费的maximum offset，并定期commit到offset manager（broker）。
- Simple consumer需要手动管理offset。现在的Simple consumer Java API只支持commit offset到zookeeper。
    

## Consumers and Consumer Groups
- consumer注册到zookeeper
- 属于同一个group的consumer（group id一样）平均分配partition，每个partition只会被一个consumer消费。
- 当broker或同一个group的其他consumer的状态发生变化的时候，consumer rebalance就会发生。
    


## Zookeeper协调控制
- 管理broker与consumer的动态加入与离开。
- 触发负载均衡，当broker或consumer加入或离开时会触发负载均衡算法，使得一个consumer group内的多个consumer的订阅负载平衡。
- 维护消费关系及每个partition的消费信息。
    
Zookeeper主要用于在集群中不同节点之间进行通信，在Kafka中，它被用于提交偏移量，因此如果节点在任何情况下都失败了，它都可以从之前提交的偏移量中获取。
除此之外，它还执行其他活动，如: leader检测、分布式同步、配置管理、识别新节点何时离开或连接、集群、节点实时状态等等。
    

## Zookeeper在kafka中的作用
- 分区leader选举
- 配置管理，topic配置的动态更新，topic、分区、leader分区所在的broker.id
- 负载均衡，基于zookeeper的消费者，实现了该特性，动态的感知分区变动，将负载使用既定策略分不到消费者身上。
- 命名服务，Broker将advertised.port和advertised.host.name这两个配置发布到zookeeper上的节点上/brokers/ids/BrokerId(broker.id)， 供生产者，消费者，其它Broker跟其建立连接用的。
- 分布式通知，分区增加，topic变动，Broker上线下线等
- offset管理，存储comsumer-group在topic 分区上的offset，commit时需要更新offset
    

## 如何用kafka保证消息的有序性
同一个分区的消息是有序的，可以利用key的hash，把有序的消息send到同一个分区去。
    

## kafka如何保证并发情况下消息只被消费一次
默认是at least once，可以先commit再消费消息。
