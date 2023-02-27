package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;

@Component
@Log4j2
public class LibraryEventsProducer {
	
	@Autowired
	KafkaTemplate<Integer,String> kafkaTemplate;
	
	@Autowired
	ObjectMapper objectMapper;
	
	String topic = "library-events";
	
	public void sendLibraryEventAsync(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key,value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
				

			}
		});
		
	}
	
	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws Exception {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.sendDefault(key,value).get(1, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException e) {
			log.error("InterruptedException/ExecutionException sending the message and the exception is {}",e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("Exception sending the message and the exception is {}",e.getMessage());
			throw e;
		}
		
		return sendResult;
	}
	
	public void sendLibraryEventAsyncUsingSend(LibraryEvent libraryEvent) throws JsonProcessingException {

		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send("library-events", key,
				value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);

			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);

			}
		});
		
	}
	
	public void sendLibraryEventAsyncUsingProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {

		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);

			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);

			}
		});
		
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
		//RecordHeaders are used to send the list of header values along with the message to the topics
		List<Header> recordHeaders = List.of(new RecordHeader(topic, "Scanner".getBytes()), new RecordHeader(topic, "Scanner2".getBytes()) );
		
		return new ProducerRecord<Integer, String>(topic, null, key, value, recordHeaders);
	}

	protected void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending the message and the exception is {}",ex.getMessage());
		try {
			throw ex;
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			log.error("Error in onFailure: {}", e.getMessage());
		}
	}

	protected void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message sent successfully for the key : {} and the value is : {}, partition is {}", key, value, result.getRecordMetadata().partition());
		
	}
	
	
}
