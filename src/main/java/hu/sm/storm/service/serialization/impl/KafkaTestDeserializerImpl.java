package hu.sm.storm.service.serialization.impl;

import java.io.IOException;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.interceptor.qualifier.EnteringExitingLogged;
import hu.sm.storm.service.serialization.api.KafkaTestDeserializer;
import test.kafkatest;

@EnteringExitingLogged
public class KafkaTestDeserializerImpl extends SerializationBase implements KafkaTestDeserializer {

	public static final Logger LOG = LoggerFactory.getLogger(KafkaTestDeserializerImpl.class);
	private static DecoderFactory decoderFactory = null;

	public KafkaTestDeserializerImpl() {
		prepare();
	}

	public KafkaMessage deserialize(byte[] data) {

		KafkaMessage msg = null;
		try {
			DatumReader<kafkatest> reader = new SpecificDatumReader<kafkatest>(schema);
			Decoder decoder = getDecoderFactory().binaryDecoder(data, null);
			kafkatest result = reader.read(null, decoder);
			msg = new KafkaMessage(result.getId(), result.getRandom(), result.getData().toString(), data.length);
		} catch (IOException e) {
			System.err.println(e.toString());
			throw new RuntimeException(e);
		}
		return msg;
	}

	private DecoderFactory getDecoderFactory() {
		if (decoderFactory == null) {
			decoderFactory = DecoderFactory.get();
		}
		return decoderFactory;
	}

}
