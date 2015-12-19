package hu.sm.storm.topology.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import hu.sm.storm.base.AppConstants;
import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.api.MessageExtractorService;
import hu.sm.storm.service.exception.MessageExtractorServiceException;
import hu.sm.storm.service.injector.InjectorProvider;
import hu.sm.storm.topology.hook.BoltMetricsHook;

@SuppressWarnings("serial")
public class MessageExtractorBolt extends BaseRichBolt {

	private static final String C_NAME = MessageExtractorBolt.class.getName();
	private static Logger LOG = LoggerFactory.getLogger(MessageExtractorBolt.class);

	private static Injector injector;
	private OutputCollector outputCollector;
	private MessageExtractorService extractorService = null;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(AppConstants.KAFKA_MESSAGE, AppConstants.TOPIC));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		injector = InjectorProvider.getInjector();
		outputCollector = collector;
		extractorService = injector.getInstance(MessageExtractorService.class);
		context.addTaskHook(new BoltMetricsHook());
	}

	@Override
	public void execute(Tuple tuple) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Entering " + C_NAME + ".execute");
		}
		try {
			byte[] data = tuple.getBinary(0);
			KafkaMessage message = extractorService.toKafkaMessageObject(data);
			String topic = "random" + message.getRandom();
			outputCollector.emit(new Values(message, topic));
			outputCollector.ack(tuple);
			if (LOG.isInfoEnabled()) {
				LOG.info("Message " + message + " has been emitted");
			}
		} catch (MessageExtractorServiceException e) {
			outputCollector.fail(tuple);
		} finally {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Exiting " + C_NAME + ".execute");
			}
		}
	}

}
