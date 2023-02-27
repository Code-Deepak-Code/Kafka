//package com.example.demo.config;
//
//import org.apache.kafka.clients.admin.NewTopic;
//import org.springframework.boot.autoconfigure.AutoConfiguration;
//import org.springframework.context.annotation.Bean;
//import org.springframework.kafka.config.TopicBuilder;
//
//@AutoConfiguration
//public class AutoCreateConfig {
//
//	@Bean
//	public NewTopic libraryEvent() {
//		
//		return TopicBuilder.name("library-events")
//					.partitions(3)
//					.replicas(3)
//					.build();
//	}
//}
