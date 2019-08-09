package com.asiainfo.kafka.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Time;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.zk.KafkaZkClient;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * Kafka consumer/producer 工具类
 * 
 * @author       zq
 * @date         2018年1月11日  上午11:38:17
 * Copyright: 	  北京亚信智慧数据科技有限公司
 */
public class KafkaUtils {
    
    // kafka消费者容监听容器，消费所有分区，最大并发数超过分区数的，超过部分无效
    public static <K extends java.io.Serializable, V extends java.io.Serializable> ConcurrentMessageListenerContainer<K, V> 
        createConcurrentContainer(ContainerProperties cp, Properties properties) {
        
        Map<String, Object> props = consumerProps(properties);
        DefaultKafkaConsumerFactory<K, V> factory = new DefaultKafkaConsumerFactory<>(props);
        ConcurrentMessageListenerContainer<K, V> container = new ConcurrentMessageListenerContainer<>(factory, cp);
        container.setConcurrency(Integer.parseInt(properties.getProperty("spring.kafka.listener.concurrency", "3")));
        return container;
    }

    // kafka消费者容监听容器，消费所有分区
    public static <K extends java.io.Serializable, V extends java.io.Serializable> KafkaMessageListenerContainer<K, V> 
        createContainer(ContainerProperties cp, Properties properties) {
        
        Map<String, Object> props = consumerProps(properties);
        DefaultKafkaConsumerFactory<K, V> factory = new DefaultKafkaConsumerFactory<>(props);
        KafkaMessageListenerContainer<K, V> container = new KafkaMessageListenerContainer<>(factory, cp);
        return container;
    }
    
    // kafkaTemplate生产者模板
    public static <K extends java.io.Serializable, V extends java.io.Serializable>  KafkaTemplate<K, V> createTemplate(Properties properties) {
        
        Map<String, Object> producerProps = producerProps(properties);
        ProducerFactory<K, V> factory = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<K, V> template = new KafkaTemplate<>(factory);
        return template;
    }

    // 发送到kafka
    public static <K extends java.io.Serializable, V extends java.io.Serializable> ListenableFuture<SendResult<K, V>> 
        send(KafkaTemplate<K, V> template, String topic, V data) {
        
        return template.send(topic, data);
    }
    
    // 发送到kafka
    public static <K extends java.io.Serializable, V extends java.io.Serializable> List<ListenableFuture<SendResult<K, V>>> 
        send(KafkaTemplate<K, V> template, String topic, List<V> list) {
        
        List<ListenableFuture<SendResult<K, V>>> result = new ArrayList<>();
        for (V message : list) {
            result.add(template.send(topic, message));
        }
        template.flush();
        return result;
    }
    
    // consumer配置项，通常从Context里读取配置
    private static Map<String, Object> consumerProps(Properties properties) {
        
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("spring.kafka.bootstrap-servers"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                forName(properties.getProperty("spring.kafka.consumer.key-deserializer"), StringDeserializer.class));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                forName(properties.getProperty("spring.kafka.consumer.value-deserializer"), StringDeserializer.class));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("spring.kafka.consumer.group-id"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        return props;
    }
    
    // producer配置项，通常从Context里读取配置
    private static Map<String, Object> producerProps(Properties properties) {
        
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("spring.kafka.bootstrap-servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                forName(properties.getProperty("spring.kafka.producer.key-serializer"), StringSerializer.class));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                forName(properties.getProperty("spring.kafka.producer.value-serializer"), StringSerializer.class));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(properties.getProperty("spring.kafka.producer.batch-size", "16384")));
        props.put(ProducerConfig.ACKS_CONFIG, Integer.parseInt(properties.getProperty("spring.kafka.producer.acks", "1")));
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        return props;
    }

    // 加载序列化类
    private static Class<?> forName(String clazzName, Class<?> defaultClass) {
        if (StringUtils.isEmpty(clazzName)) {
            return defaultClass;
        }
        Class<?> result = defaultClass;
        try {
            result = Class.forName(clazzName);
        } catch (Exception ex) {
            // ignore
        }
        return result;
    }
    
    public static final int ZK_SESSION_TIMEOUT = 30000;
    public static final int ZK_CONNECTION_TIMEOUT = 30000;
    
    // 从zookeeper中读取Bootstrapserver
    public static String lookupBootstrap(String zookeeperConnect, SecurityProtocol securityProtocol) {
        
        try (KafkaZkClient zkClient = KafkaZkClient.apply(zookeeperConnect,
                JaasUtils.isZkSecurityEnabled(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, 10,
                Time.SYSTEM, "kafka.server", "SessionExpireListener")) {
            
            List<Broker> brokerList = JavaConverters.seqAsJavaListConverter(zkClient.getAllBrokersInCluster()).asJava();
            List<BrokerEndPoint> endPoints = brokerList.stream()
                    .map(broker -> broker.brokerEndPoint(ListenerName.forSecurityProtocol(securityProtocol)))
                    .collect(Collectors.toList());
            List<String> connections = new ArrayList<>();
            for (BrokerEndPoint endPoint : endPoints) {
                connections.add(endPoint.connectionString());
            }
            return StringUtils.collectionToCommaDelimitedString(connections);
        }
    }
    
    // 从zookeeper中migrate topic offset到broker中
    public static void migrateOffsets(String zookeeperConnect, String topic, String groupId, Properties properties) {
        
        try (KafkaZkClient zkClient = KafkaZkClient.apply(zookeeperConnect, 
                JaasUtils.isZkSecurityEnabled(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, 10,
                Time.SYSTEM, "kafka.server", "SessionExpireListener");
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps(properties))) {
            
            // 读取broker中的consumer offset
            Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = getTopicOffsets(consumer, topic);
            if (kafkaOffsets == null || kafkaOffsets.isEmpty()) {
                return;
            }
            // No Kafka offsets found. Migrating zookeeper offsets
            Map<TopicPartition, OffsetAndMetadata> zookeeperOffsets = getZookeeperOffsets(zkClient, consumer, topic, groupId);
            if (zookeeperOffsets.isEmpty()) {
                return;
            }
            // Committing Zookeeper offsets to broker
            consumer.commitSync(zookeeperOffsets);
        }
    }
    
    // 从zookeeper读取topic 各分区的当前offset
    public static  Map<TopicPartition, OffsetAndMetadata> getZookeeperOffsets(
            KafkaZkClient zkClient, KafkaConsumer<String, byte[]> consumer, String topic, String groupId) {

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        for (PartitionInfo partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
            Option<Object> optionOffset = zkClient.getConsumerOffset(groupId, topicPartition);
            if (optionOffset.nonEmpty()) {
                Long offset = (Long) optionOffset.get();
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                offsets.put(topicPartition, offsetAndMetadata);
           }
        }
        return offsets;
    }
    
    // 从consumer中读取topic所有分区上一次commit的offset
    public static  Map<TopicPartition, OffsetAndMetadata> getTopicOffsets(KafkaConsumer<String, byte[]> consumer, String topic) {
        
        Map<TopicPartition, OffsetAndMetadata> offsets = null;
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        if (partitions != null) {
            offsets = new HashMap<>();
            for (PartitionInfo partition : partitions) {
                TopicPartition tp = new TopicPartition(topic, partition.partition());
                OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
                if (offsetAndMetadata != null) {
                    offsets.put(tp, offsetAndMetadata);
                }
            }
        }
        return offsets;
    }
}
