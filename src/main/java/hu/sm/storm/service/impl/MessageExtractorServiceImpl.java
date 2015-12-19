package hu.sm.storm.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.api.MessageExtractorService;
import hu.sm.storm.service.exception.MessageExtractorServiceException;
import hu.sm.storm.service.interceptor.qualifier.EnteringExitingLogged;
import hu.sm.storm.service.serialization.api.KafkaTestDeserializer;
import hu.sm.storm.service.serialization.api.KafkaTestSerializer;

@EnteringExitingLogged
public class MessageExtractorServiceImpl implements MessageExtractorService {

	private static Logger LOG = LoggerFactory.getLogger(MessageExtractorServiceImpl.class);
	private KafkaTestDeserializer deserializer;
	private KafkaTestSerializer serializer;

	@Inject
	public MessageExtractorServiceImpl(KafkaTestDeserializer aDeserializer, KafkaTestSerializer aSerializer) {
		this.deserializer = aDeserializer;
		this.serializer = aSerializer;
	}

	@Override
	public KafkaMessage toKafkaMessageObject(byte[] data) throws MessageExtractorServiceException {
		try {
			KafkaMessage result = deserializer.deserialize(data);
			return result;
		} catch (Exception e) {
			String msg = "Converting byte array to KafkaMessage failed";
			LOG.error(msg, e);
			throw new MessageExtractorServiceException(msg, e);
		}
	}

	@Override
	public byte[] toByteArray(KafkaMessage message) throws MessageExtractorServiceException {
		try {
			byte[] result = serializer.serialize(message);
			return result;
		} catch (Exception e) {
			String msg = "Converting KafkMessage to byte array failed";
			LOG.error(msg, e);
			throw new MessageExtractorServiceException(msg, e);
		}
	}

}
