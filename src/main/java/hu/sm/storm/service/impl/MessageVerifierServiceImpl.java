package hu.sm.storm.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.api.MessageVerifierService;
import hu.sm.storm.service.interceptor.qualifier.EnteringExitingLogged;

@EnteringExitingLogged
public class MessageVerifierServiceImpl implements MessageVerifierService {

	private static Logger LOG = LoggerFactory.getLogger(MessageVerifierServiceImpl.class);

	@Override
	public boolean nrOfMessagesMatchsOnAllTopic(Map<String, Set<KafkaMessage>> rootTopicMessagesByTopicId,
			Map<String, Set<KafkaMessage>> randomTopicMessagesByTopicId) {
		boolean result = true;
		if (rootTopicMessagesByTopicId == null || randomTopicMessagesByTopicId == null) {
			return result = false;
		}
		for (String topic : rootTopicMessagesByTopicId.keySet()) {
			Set<KafkaMessage> messagesOnRoot = rootTopicMessagesByTopicId.get(topic);
			Set<KafkaMessage> messagesOnRandom = randomTopicMessagesByTopicId.get(topic);
			int rootTopicCount = 0;
			if (messagesOnRoot != null) {
				rootTopicCount = messagesOnRoot.size();
			}
			int randomTopicCount = 0;
			if (messagesOnRandom != null) {
				randomTopicCount = messagesOnRandom.size();
			}
			result = result && (rootTopicCount == randomTopicCount);
			if ((rootTopicCount != randomTopicCount)) {
				LOG.info("Haven't received all messages on topic " + topic);
			}
		}

		return result;
	}

	@Override
	public boolean verifyMessages(Map<String, Set<KafkaMessage>> messagesFromRootopic,
			Map<String, Set<KafkaMessage>> messagesFromRandomTopic) {
		boolean result = true;
		List<KafkaMessage> allMessagesOnRoot = new ArrayList<>();
		for (String topic : messagesFromRootopic.keySet()) {
			Set<KafkaMessage> messages = messagesFromRootopic.get(topic);
			allMessagesOnRoot.addAll(messages);
		}
		Collections.sort(allMessagesOnRoot);
		for (KafkaMessage message : allMessagesOnRoot) {
			String expectedTopic = message.getTargetTopic();
			Set<KafkaMessage> messagesOnExpectedTopic = messagesFromRandomTopic.get(expectedTopic);
			boolean found = messagesOnExpectedTopic.contains(message);
			logResult(message, expectedTopic, found);
			result = result && found;
		}
		return result;
	}

	private void logResult(KafkaMessage message, String expectedTopic, boolean found) {
		if (LOG.isInfoEnabled()) {
			String foundOrNot = found ? "" : "NOT";
			String msg = String.format("Message with id: %d and random :%d  has %s been received on topic %s",
					message.getId(), message.getRandom(), foundOrNot, expectedTopic);
			LOG.info(msg);
		}
	}
}
