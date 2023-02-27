package com.learnkafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventsProducer;

import lombok.extern.log4j.Log4j2;

@Log4j2
@RestController
public class LibraryEventsController {
	
	@Autowired
	LibraryEventsProducer libraryEventsProducer;
	
	//Async way of producing message
	 @PostMapping("/v1/async/libraryEvent")
	    public ResponseEntity<LibraryEvent> postLibraryEventAsync(@RequestBody LibraryEvent libraryEvent) throws Exception {
	    	
		 	libraryEvent.setLibraryEventType(LibraryEventType.NEW);
	    	libraryEventsProducer.sendLibraryEventAsync(libraryEvent);
	    		    	
	        //invoke kafka producer
	        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	    }
	 
	//sync way of producing message
    @PostMapping("/v1/sync/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws Exception {
    	
    	libraryEvent.setLibraryEventType(LibraryEventType.NEW);
//    	libraryEventsProducer.sendLibraryEvent(libraryEvent);
    	SendResult<Integer, String> sendResult = libraryEventsProducer.sendLibraryEventSynchronous(libraryEvent);
    	
    	log.info("Send result is {} ",sendResult.toString());
    	
        //invoke kafka producer
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
    
  //ProducerRecord way of producing message
    @PostMapping("/v1/producerrecord/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEventUsingProducerRecord(@RequestBody LibraryEvent libraryEvent) throws Exception {
    	
    	libraryEvent.setLibraryEventType(LibraryEventType.NEW);
//    	libraryEventsProducer.sendLibraryEvent(libraryEvent);
    	libraryEventsProducer.sendLibraryEventAsyncUsingProducerRecord(libraryEvent);
    	
        //invoke kafka producer
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
    
  //ProducerRecord way of producing message
    @PutMapping("/v1/producerrecord/libraryEvent")
    public ResponseEntity<?> putLibraryEventUsingProducerRecord(@RequestBody LibraryEvent libraryEvent) throws Exception {
    	
    	if(libraryEvent.getLibraryEventId() == null) {
    		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the libraryEventId");
    	}
    	libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
//    	libraryEventsProducer.sendLibraryEvent(libraryEvent);
    	libraryEventsProducer.sendLibraryEventAsyncUsingProducerRecord(libraryEvent);
    	
        //invoke kafka producer
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
    
}
