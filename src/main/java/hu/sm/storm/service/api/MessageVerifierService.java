package hu.sm.storm.service.api;

import java.util.Map;
import java.util.Set;

import hu.sm.storm.base.domain.KafkaMessage;

public interface MessageVerifierService {

	public boolean nrOfMessagesMatchsOnAllTopic(Map<String,Set<KafkaMessage>> messagesFromRootopic,Map<String,Set<KafkaMessage>> messagesFromRandomTopic);
	public boolean verifyMessages(Map<String,Set<KafkaMessage>> messagesFromRootopic,Map<String,Set<KafkaMessage>> messagesFromRandomTopic); 
}
