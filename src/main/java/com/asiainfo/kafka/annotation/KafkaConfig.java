package com.asiainfo.kafka.annotation;

import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/**
 * @Description: KafkaAutoConfiguration 自动配置kafka消费容器、KafkaTemplate，默认配置为 ConcurrentKafkaListenerContainerFactory，并自动激活 @EnableKafka
 * 
 *               @EnableKafka 自动注册bean里标注 @KafkaListener 的方法为容器消费监听器
 *               @KafkaListener 标注一个方法为指定topic的消费者
 * 
 * @author chenzq  
 * @date 2019年8月9日 下午12:26:01
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved.
 */
@Profile("kafka")
@Configuration
@EnableKafka
public class KafkaConfig {
	
	@Bean
	@Primary
	@Qualifier("kafkaProperties")
	@ConfigurationProperties(prefix = "spring.kafka")
	public KafkaProperties kafkaProperties() {
		return new KafkaProperties();
	}
	
	@Bean
	@Qualifier("consumerFactory")
	public DefaultKafkaConsumerFactory<String, String> consumerFactory(@Qualifier("kafkaProperties") KafkaProperties properties) {
		return new DefaultKafkaConsumerFactory<String, String>(properties.buildConsumerProperties());
	}

	@Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory(
    		@Qualifier("kafkaProperties") KafkaProperties properties, 
    		@Qualifier("consumerFactory") DefaultKafkaConsumerFactory<String, String> consumerFactory) {
		
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.getListener().getConcurrency());
        factory.getContainerProperties().setPollTimeout(properties.getListener().getPollTimeout());
        return factory;
    }
	
	@Bean
	public KafkaTemplate<String, String> createTemplate(@Qualifier("kafkaProperties") KafkaProperties properties) {
		Map<String, Object> props = properties.buildProducerProperties();
		ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<String, String>(props);
		return new KafkaTemplate<String, String>(pf);
	}
}
