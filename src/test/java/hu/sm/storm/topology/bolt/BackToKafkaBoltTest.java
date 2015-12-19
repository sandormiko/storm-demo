package hu.sm.storm.topology.bolt;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import hu.sm.storm.service.api.MessageExtractorService;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

@RunWith(MockitoJUnitRunner.class)
public class BackToKafkaBoltTest extends TestCaseBase{

	@Mock
	private OutputCollector collector;
	@Mock
	private MessageExtractorService extractorService;

	@Mock
	private Producer<String, byte[]> producer;
	private Tuple input = null;

	@Before
	public void init() {
		input = prepareTuple();
	}

	@Test
	public void shouldAckWhenMessageReceived() {
		BackToKafkaBolt backToKafkaBolt = new BackToKafkaBolt(producer, extractorService, collector);
		backToKafkaBolt.execute(input);
		Mockito.verify(collector, Mockito.times(1)).ack(Mockito.any(Tuple.class));

	}

	@Test
	public void shouldProduceMessageAfterMessageReceived() {
		BackToKafkaBolt backToKafkaBolt = new BackToKafkaBolt(producer, extractorService, collector);
		backToKafkaBolt.execute(input);
		Mockito.verify(producer, Mockito.times(1)).send(Mockito.any(KeyedMessage.class));
	}
}
