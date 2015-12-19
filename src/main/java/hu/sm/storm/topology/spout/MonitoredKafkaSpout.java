package hu.sm.storm.topology.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import hu.sm.storm.topology.hook.SpoutMetricsHook;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

@SuppressWarnings("serial")
public class MonitoredKafkaSpout extends KafkaSpout {

	public MonitoredKafkaSpout(SpoutConfig spoutConf) {
		super(spoutConf);

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		context.addTaskHook(new SpoutMetricsHook());
	}

}
