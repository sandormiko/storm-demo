package hu.sm.storm.service.api;

import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.exception.MessageExtractorServiceException;

public interface MessageExtractorService {

	public KafkaMessage toKafkaMessageObject(byte[] data) throws MessageExtractorServiceException;
	public byte[] toByteArray(KafkaMessage message) throws MessageExtractorServiceException;
}
