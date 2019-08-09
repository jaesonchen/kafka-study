package com.asiainfo.kafka.annotation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * kafka listener：可指定多个topic、topic正则表达式、topic分区进行消费。
 * 
 * @author       zq
 * @date         2017年11月2日  下午3:57:26
 * Copyright: 	  北京亚信智慧数据科技有限公司
 */
@Profile("kafka")
@Component
public class MessageListener {

    final Logger logger = LoggerFactory.getLogger(getClass());
    
    // 指定topic名称，可指定多个
	@KafkaListener(topics = {"com.asiainfo.request"})
    public void processMessage(String message) {
	    logger.info("message from topic(com.asiainfo.request): {}", message);
	}
	
	// 指定topic 正则表达式
	@KafkaListener(topicPattern = "*response$")
    public void processMessage1(String message) {
		logger.info("message from topic(storm): {}", message);
	}
	
	// 指定topic和分区
	@KafkaListener(topicPartitions = { @TopicPartition(topic = "com.asiainfo.request", partitions = { "0", "2" } )})
	public void processMessage2(String message) {
        logger.info("message from topic(com.asiainfo.request: 0/2): {}", message);
    }
	
    // 指定topic和分区
    @KafkaListener(topicPartitions = { @TopicPartition(topic = "com.asiainfo.request", partitions = { "1" } )})
    public void processMessage3(String message) {
        logger.info("message from topic(com.asiainfo.request: 1): {}", message);
    }
}
