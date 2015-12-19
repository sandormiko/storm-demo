package hu.sm.storm.service.serialization.impl;

import java.io.IOException;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SerializationBase {

	public static final Logger LOG = LoggerFactory.getLogger(SerializationBase.class);
	protected transient Schema schema = null;

	public void prepare() {
		Schema.Parser parser = new Schema.Parser();
		try {
			schema = parser.parse(getClass().getResourceAsStream("/kafkatestSchema.avsc"));
		} catch (IOException e) {
			String msg = "Caught IOException while initializing avro schema";
			LOG.error(msg, e);
			throw new RuntimeException(e);
		}
	}
}
