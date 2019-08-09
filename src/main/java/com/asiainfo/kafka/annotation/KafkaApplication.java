package com.asiainfo.kafka.annotation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * TODO
 * 
 * @author       zq
 * @date         2017年11月3日  上午11:16:09
 * Copyright: 	  北京亚信智慧数据科技有限公司
 */
@SpringBootApplication
@ComponentScan("com.asiainfo.kafka.annotation")
@RestController
public class KafkaApplication {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;
	
	@RequestMapping("/produce/request/{message}")
	String produce(@PathVariable("message") String message) {
		kafkaTemplate.send("com.asiainfo.request", message);
		return "success";
	}
	
	@RequestMapping("/produce/response/{message}")
	String produce1(@PathVariable("message") String message) {
		kafkaTemplate.send("com.asiainfo.response", message);
		return "success";
	}
	
	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(new Object[] { KafkaApplication.class });
		app.setAdditionalProfiles(new String[] { "kafka" });
		app.run(args);
	}
}
