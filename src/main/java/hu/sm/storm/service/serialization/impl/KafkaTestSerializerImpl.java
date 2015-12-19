package hu.sm.storm.service.serialization.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.interceptor.qualifier.EnteringExitingLogged;
import hu.sm.storm.service.serialization.api.KafkaTestSerializer;
import test.kafkatest;

@EnteringExitingLogged
public class KafkaTestSerializerImpl extends SerializationBase implements KafkaTestSerializer {

	public static final Logger LOG = LoggerFactory.getLogger(KafkaTestSerializerImpl.class);
	private static EncoderFactory encoderFactory = null;

	public KafkaTestSerializerImpl() {
		prepare();
	}

	public byte[] serialize(KafkaMessage message) {
		byte[] result = null;
		ByteArrayOutputStream out = null;
		try {
			kafkatest value = new kafkatest(message.getId(), message.getRandom(), message.getData());
			DatumWriter<kafkatest> writer = new SpecificDatumWriter<kafkatest>(schema);
			out = new ByteArrayOutputStream();
			Encoder encoder = getEncoderFactory().binaryEncoder(out, null);
			writer.write(value, encoder);
			encoder.flush();
			result = out.toByteArray();
		} catch (IOException e) {
			System.err.println(e.toString());
			throw new RuntimeException(e);
		} finally {
			if (out != null) {
				CloseableUtils.closeQuietly(out);
			}
		}
		return result;
	}

	private EncoderFactory getEncoderFactory() {
		if (encoderFactory == null) {
			encoderFactory = EncoderFactory.get();
		}
		return encoderFactory;
	}

}
