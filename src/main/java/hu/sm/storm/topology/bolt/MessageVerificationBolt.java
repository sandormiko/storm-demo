package hu.sm.storm.topology.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import hu.sm.storm.base.AppConstants;
import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.api.MessageVerifierService;
import hu.sm.storm.service.injector.InjectorProvider;
import hu.sm.storm.topology.hook.BoltMetricsHook;

@SuppressWarnings("serial")
public class MessageVerificationBolt extends BaseRichBolt {

	private static final String C_NAME = MessageVerificationBolt.class.getName();
	private static Logger LOG = LoggerFactory.getLogger(MessageVerificationBolt.class);
	private static Injector injector = null;

	private Map<String, Set<KafkaMessage>> rootTopicMessagesByTopicId = null;
	private Map<String, Set<KafkaMessage>> randomTopicMessagesByTopicId = null;
	private OutputCollector collector = null;
	private MessageVerifierService verifierSrv;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector aCollector) {
		this.collector = aCollector;
		randomTopicMessagesByTopicId = new HashMap<>();
		rootTopicMessagesByTopicId = new HashMap<>();
		injector = InjectorProvider.getInjector();
		verifierSrv = injector.getInstance(MessageVerifierService.class);
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
				Set<KafkaMessage> messagesOnTopic = getMessagesByTopic(rootTopicMessagesByTopicId, topic);
				messagesOnTopic.add(msg);
				rootTopicMessagesByTopicId.put(topic, messagesOnTopic);
			} else {
				Set<KafkaMessage> messagesOnTopic = getMessagesByTopic(randomTopicMessagesByTopicId, topic);
				messagesOnTopic.add(msg);
				randomTopicMessagesByTopicId.put(topic, messagesOnTopic);
			}
			boolean nrOfMessagesMatchesOnAllTopic = verifierSrv.nrOfMessagesMatchsOnAllTopic(rootTopicMessagesByTopicId,
					randomTopicMessagesByTopicId);
			boolean allMessagesHasBeenVerified = false;
			if (nrOfMessagesMatchesOnAllTopic) {
				allMessagesHasBeenVerified = verifierSrv.verifyMessages(rootTopicMessagesByTopicId,
						randomTopicMessagesByTopicId);
			}
			if (LOG.isInfoEnabled()) {
				LOG.info("All messages have been verified " + allMessagesHasBeenVerified);
			}
			if(allMessagesHasBeenVerified){
				clearCache();
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

	private Set<KafkaMessage> getMessagesByTopic(Map<String, Set<KafkaMessage>> messagesbyTopicMap, String topic) {
		Set<KafkaMessage> messagesOnTopic = messagesbyTopicMap.get(topic);
		if (messagesOnTopic == null) {
			messagesOnTopic = new HashSet<>();
		}
		return messagesOnTopic;
	}
	
	private void clearCache(){
		randomTopicMessagesByTopicId.clear();
		rootTopicMessagesByTopicId.clear();
	}
}
