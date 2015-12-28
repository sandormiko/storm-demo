package hu.sm.storm.topology.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import hu.sm.storm.base.AppConstants;
import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.topology.hook.BoltMetricsHook;

@SuppressWarnings("serial")
public class MessageVerificationBolt extends BaseRichBolt {

	private static final String C_NAME = MessageVerificationBolt.class.getName();
	private static Logger LOG = LoggerFactory.getLogger(MessageVerificationBolt.class);

	private Map<String, Set<KafkaMessage>> rootTopicMessagesByTopicId = null;
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector aCollector) {
		this.collector = aCollector;
		rootTopicMessagesByTopicId = new HashMap<>();
		context.addTaskHook(new BoltMetricsHook());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Entering " + C_NAME + ".execute");
		}
		try {
			String topic = input.getStringByField(AppConstants.TOPIC);
			KafkaMessage msg = (KafkaMessage) input.getValueByField(AppConstants.KAFKA_MESSAGE);
			if (input.getSourceComponent().equals(AppConstants.KAFKA_MSG_EXTRACTOR)) {
				Set<KafkaMessage> messagesOnTopic = getMessagesByTopic(topic);
				messagesOnTopic.add(msg);
				rootTopicMessagesByTopicId.put(topic, messagesOnTopic);
			} else {
				Set<KafkaMessage> messagesOnTopic = getMessagesByTopic(topic);
				boolean removed = messagesOnTopic.remove(msg);
				if (!removed) {
					String infoMsg = String.format("Message %s has not been received on topic %s", msg.toString(), topic);
					LOG.warn(infoMsg);
				}
				
				if (messagesOnTopic.size() == 0) {
					if (LOG.isInfoEnabled()) {
						LOG.info("All messages have been received on topic " + topic);
					}
				}
			}

			collector.ack(input);
		} finally {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Exiting " + C_NAME + ".execute");
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No output fields to declare
	}

	private Set<KafkaMessage> getMessagesByTopic(String topic) {
		Set<KafkaMessage> messagesOnTopic = rootTopicMessagesByTopicId.get(topic);
		if (messagesOnTopic == null) {
			messagesOnTopic = new HashSet<>();
		}
		return messagesOnTopic;
	}

}
