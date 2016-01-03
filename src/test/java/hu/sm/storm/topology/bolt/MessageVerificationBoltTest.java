package hu.sm.storm.topology.bolt;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@RunWith(MockitoJUnitRunner.class)
public class MessageVerificationBoltTest extends TestCaseBase {

	@Mock
	private OutputCollector collector;
	@Mock
	private TopologyContext topologyContext;
	@Mock
	private OutputFieldsDeclarer declarer;

	@Test
	public void shouldEmitValuesWhenMessageReceived() throws IOException {
		Tuple input = prepareTuple();
		MessageVerificationBolt aggregator = new MessageVerificationBolt();
		aggregator.prepare(new Config(), topologyContext, collector);
		aggregator.execute(input);

		Mockito.verify(collector).ack(Mockito.any(Tuple.class));
	}

}
