package hu.sm.storm.topology.bolt;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import hu.sm.storm.base.AppConstants;
import hu.sm.storm.base.ApplicationProperties;
import hu.sm.storm.base.domain.KafkaMessage;
import hu.sm.storm.service.api.MessageExtractorService;
import hu.sm.storm.service.exception.MessageExtractorServiceException;
import hu.sm.storm.service.injector.InjectorProvider;
import hu.sm.storm.topology.hook.BoltMetricsHook;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

@SuppressWarnings("serial")
public class BackToKafkaBolt extends BaseRichBolt {

	private static final String C_NAME = BackToKafkaBolt.class.getName();
	private static Logger LOG = LoggerFactory.getLogger(BackToKafkaBolt.class);
	private static Injector injector;

	private MessageExtractorService extractorService;
	private Producer<String, byte[]> producer;
	private OutputCollector outputCollector;

	public BackToKafkaBolt() {

	}

	public BackToKafkaBolt(Producer<String, byte[]> aProducer, MessageExtractorService anExtrSrv,
			OutputCollector collector) {
		this.producer = aProducer;
		this.extractorService = anExtrSrv;
		this.outputCollector = collector;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		injector = InjectorProvider.getInjector();
		ProducerConfig config = prepareProducerConfig();
		producer = new Producer<>(config);
		outputCollector = collector;
		extractorService = injector.getInstance(MessageExtractorService.class);
		context.addTaskHook(new BoltMetricsHook());
	}

	private ProducerConfig prepareProducerConfig() {
		ApplicationProperties appProps = ApplicationProperties.getInstance();
		Properties props = new Properties();
		props.put("metadata.broker.list", appProps.getBrokerProperties());
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		return config;
	}

	@Override
	public void execute(Tuple input) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Entering " + C_NAME + ".execute");
		}
		KafkaMessage result = null;
		try {
			result = (KafkaMessage) input.getValueByField(AppConstants.KAFKA_MESSAGE);
			byte[] data = extractorService.toByteArray(result);
			String topic = (String) input.getStringByField(AppConstants.TOPIC);
			KeyedMessage<String, byte[]> keyedMessage = new KeyedMessage<String, byte[]>(topic, data);
			producer.send(keyedMessage);
			outputCollector.ack(input);
			if (LOG.isInfoEnabled()) {
				LOG.info("Message " + result + " has been processed");
			}
		} catch (MessageExtractorServiceException e) {
			outputCollector.fail(input);
		} finally {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Exiting " + C_NAME + ".execute");
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No output fields to define
	}

	@Override
	public void cleanup() {
		producer.close();
	}

}
