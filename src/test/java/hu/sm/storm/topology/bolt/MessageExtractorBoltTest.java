package hu.sm.storm.topology.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import backtype.storm.Config;
import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.serialization.api.KafkaTestSerializer;
import hu.sm.storm.service.serialization.impl.KafkaTestSerializerImpl;

@RunWith(MockitoJUnitRunner.class)
public class MessageExtractorBoltTest {

	@Mock
	private OutputCollector collector;
	@Mock
	private TopologyContext topologyContext;

	@Mock
	private OutputFieldsDeclarer declarer;
	private KafkaTestSerializer serializer = null;

	@Before
	public void init() {
		serializer = new KafkaTestSerializerImpl();
	}

	protected Tuple getTuple(KafkaMessage message) {
		byte[] data = serializer.serialize(message);
		List<byte[]> tuples = new ArrayList<>();
		tuples.add(data);
		Tuple tuple = Testing.testTuple(tuples);
		return tuple;
	}

	@Test
	public void shouldEmitValuesWhenMessageReceived() throws IOException {
		KafkaMessage message = new KafkaMessage(1, 2, "Data", 1);
		Tuple tuple = getTuple(message);
		MessageExtractorBolt metricsBolt = new MessageExtractorBolt();
		metricsBolt.prepare(new Config(), topologyContext, collector);

		metricsBolt.execute(tuple);

		Mockito.verify(collector).emit(Mockito.any(Values.class));
	}

	@Test
	public void shouldDeclareOutputFields() throws IOException {
		MessageExtractorBolt metricsBolt = new MessageExtractorBolt();

		metricsBolt.declareOutputFields(declarer);

		Mockito.verify(declarer).declare(Mockito.any(Fields.class));
	}

}
