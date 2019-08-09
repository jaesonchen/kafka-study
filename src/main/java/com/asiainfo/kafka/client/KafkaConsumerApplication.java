package com.asiainfo.kafka.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**   
 * @Description: TODO
 * 
 * @author chenzq  
 * @date 2019年8月9日 下午2:38:33
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class KafkaConsumerApplication {

    public static void main(String[] args) {
        
        Consumer<String, String> consumer = createConsumer(consumerProp(), new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Arrays.asList(new String[] { "com.asiainfo.response" }), new SimpleRebalanceListener());
        System.out.println(consumer.subscription());
        
        Map<TopicPartition, OffsetAndMetadata> tpAndOffsetMetadata = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    System.out.println(record);
                }
                System.out.println(consumer.committed(tp));
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                tpAndOffsetMetadata.put(tp, new OffsetAndMetadata(lastOffset + 1));
            }
            if (!tpAndOffsetMetadata.isEmpty()) {
                consumer.commitSync(tpAndOffsetMetadata);
                tpAndOffsetMetadata.clear();
            }
        }
    }
    
    public static Map<String, Object> consumerProp() {
        
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.103:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        return props;
    }
    
    public static Consumer<String, byte[]> createConsumer(Map<String, Object> properties) {
        return new KafkaConsumer<String, byte[]>(properties);
    }
    
    public static <K, V> Consumer<K, V> createConsumer(Map<String, Object> properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        return new KafkaConsumer<K, V>(properties, keyDeserializer, valueDeserializer);
    }
    
    // rebalance Listener
    static class SimpleRebalanceListener implements ConsumerRebalanceListener {
        
        final Logger log = LoggerFactory.getLogger(getClass());

        // Set a flag that a rebalance has occurred. Then commit already read events to kafka.
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
            }
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
            }
        }
    }
}
