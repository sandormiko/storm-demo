package hu.sm.storm.service.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.impl.MessageVerifierServiceImpl;

public class MessageVerifierServiceTest {

	private MessageVerifierService verifierService;
	Map<String, Set<KafkaMessage>> rootTopicMap ;
	Map<String, Set<KafkaMessage>> randomTopicMap ;
	
	@Before
	public void init(){
		verifierService = new MessageVerifierServiceImpl();
		rootTopicMap =  new HashMap<>();
		randomTopicMap =  new HashMap<>();
	}
	
	@Test
	public void shouldVerifiyThatAllMessagesHaveBeenReceived(){
		KafkaMessage m1 = new KafkaMessage(1, 1, "Test", 1);
		KafkaMessage m2 = new KafkaMessage(2, 1, "Test", 1);
		Set<KafkaMessage> messages = new HashSet<>();
		messages.add(m1);
		messages.add(m2);
		
		rootTopicMap.put("random1", messages);
		randomTopicMap.put("random1", messages);
		boolean result = verifierService.verifyMessages(rootTopicMap, randomTopicMap);
		Assert.assertEquals(result, true);
	}
	
	@Test
	public void shouldNotVerifiyThatAllMessagesHaveBeenReceived(){
		KafkaMessage m1 = new KafkaMessage(1, 2, "Test", 1);
		Set<KafkaMessage> messages = new HashSet<>();
		messages.add(m1);
		rootTopicMap.put("random2", messages);
		randomTopicMap.put("random2", Collections.EMPTY_SET);
		
		boolean result = verifierService.verifyMessages(rootTopicMap, randomTopicMap);
		Assert.assertEquals(result, false);
	}
	
	@Test
	public void shouldVerifiyThatTopicCountMatches(){
		KafkaMessage m1 = new KafkaMessage(1, 2, "Test", 1);
		Set<KafkaMessage> messages = new HashSet<>();
		messages.add(m1);
		
		rootTopicMap.put("random2", messages);
		randomTopicMap.put("random2", messages);
		
		rootTopicMap.put("random3", messages);
		randomTopicMap.put("random3", messages);
		
		boolean result = verifierService.nrOfMessagesMatchsOnAllTopic(rootTopicMap, randomTopicMap);
		
		Assert.assertEquals(result, true);
	}
	
	@Test
	public void shouldNotVerifiyThatTopicCountMatches(){
		KafkaMessage m1 = new KafkaMessage(1, 2, "Test", 1);
		Set<KafkaMessage> messages = new HashSet<>();
		messages.add(m1);
		
		rootTopicMap.put("random2", messages);
		
		rootTopicMap.put("random3", messages);
		randomTopicMap.put("random3", messages);
		
		boolean result = verifierService.nrOfMessagesMatchsOnAllTopic(rootTopicMap, randomTopicMap);
		
		Assert.assertEquals(result, false);
	}
}
