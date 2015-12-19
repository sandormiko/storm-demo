package hu.sm.storm.service.serialization.api;

import hu.sm.storm.base.domain.KafkaMessage;

public interface KafkaTestSerializer {

	public byte[] serialize(KafkaMessage message);
}
