package hu.sm.storm.service.serialization.api;

import hu.sm.storm.base.domain.KafkaMessage;

public interface KafkaTestDeserializer {

	public KafkaMessage deserialize(byte[] data);
}
