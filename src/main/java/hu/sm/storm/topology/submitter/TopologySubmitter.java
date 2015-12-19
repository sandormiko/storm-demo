package hu.sm.storm.topology.submitter;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import hu.sm.storm.base.AppConstants;
import hu.sm.storm.base.ApplicationProperties;
import hu.sm.storm.topology.bolt.BackToKafkaBolt;
import hu.sm.storm.topology.bolt.MessageExtractorBolt;
import hu.sm.storm.topology.bolt.MessageVerificationBolt;
import hu.sm.storm.topology.spout.MonitoredKafkaSpout;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;

public class TopologySubmitter {
	
	private static final String C_NAME = TopologySubmitter.class.getName();
	private static Logger LOG = LoggerFactory.getLogger(TopologySubmitter.class);
	private static final String KAFKA_MSG_SENDER = "kafka-msg-sender";

	public StormTopology buildTopology(ApplicationProperties appProps) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Entering " + C_NAME + ".execute");
		}
		try {
			String kafkaZookeeper = appProps.getZookeeperUrl();
			String rootTopic = appProps.getRootTopic();
			String rootTopicListener = rootTopic + "-listener";

			BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, rootTopic, "", "storm");
			TopologyBuilder builder = new TopologyBuilder();

			builder.setSpout(rootTopicListener, new MonitoredKafkaSpout(kafkaConfig));

			builder.setBolt(AppConstants.KAFKA_MSG_EXTRACTOR, new MessageExtractorBolt())
					.shuffleGrouping(rootTopicListener);
			builder.setBolt(KAFKA_MSG_SENDER, new BackToKafkaBolt()).shuffleGrouping(AppConstants.KAFKA_MSG_EXTRACTOR);

			String targetTopic1 = appProps.getTargetTopic1();
			String targetTopic2 = appProps.getTargetTopic2();
			String targetTopic3 = appProps.getTargetTopic3();
			String targetTopic1Listener = targetTopic1 + "-listener";
			String targetTopic2Listener = targetTopic2 + "-listener";
			String targetTopic3Listener = targetTopic3 + "-listener";
			SpoutConfig kafkaConfig1 = new SpoutConfig(brokerHosts, targetTopic1, "/" + targetTopic1, targetTopic1);
			builder.setSpout(targetTopic1Listener, new MonitoredKafkaSpout(kafkaConfig1));
			SpoutConfig kafkaConfig2 = new SpoutConfig(brokerHosts, targetTopic2, "/" + targetTopic2, targetTopic2);
			builder.setSpout(targetTopic2Listener, new MonitoredKafkaSpout(kafkaConfig2));
			SpoutConfig kafkaConfig3 = new SpoutConfig(brokerHosts, appProps.getTargetTopic3(), "/" + targetTopic3,
					targetTopic3);
			builder.setSpout(targetTopic3Listener, new MonitoredKafkaSpout(kafkaConfig3));

			String targetTopic1Extractor = targetTopic1 + "-extractor";
			String targetTopic2Extractor = targetTopic2 + "-extractor";
			String targetTopic3Extractor = targetTopic3 + "-extractor";
			builder.setBolt(targetTopic1Extractor, new MessageExtractorBolt()).shuffleGrouping(targetTopic1Listener);
			builder.setBolt(targetTopic2Extractor, new MessageExtractorBolt()).shuffleGrouping(targetTopic2Listener);
			builder.setBolt(targetTopic3Extractor, new MessageExtractorBolt()).shuffleGrouping(targetTopic3Listener);

			builder.setBolt(AppConstants.KAFKA_MSG_AGGREGATOR, new MessageVerificationBolt())
					.shuffleGrouping(AppConstants.KAFKA_MSG_EXTRACTOR).shuffleGrouping(targetTopic3Extractor)
					.shuffleGrouping(targetTopic2Extractor).shuffleGrouping(targetTopic1Extractor);

			return builder.createTopology();
		} finally {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Exiting " + C_NAME + ".execute");
			}
		}
	}

	public void submitTopology() throws InterruptedException {
		ApplicationProperties appProps = ApplicationProperties.getInstance();
		StormTopology stormTopology = this.buildTopology(appProps);
		String kafkaZk = appProps.getZookeeperUrl();
		String brokerList = appProps.getBrokerProperties();
		Map<String, String> kafkaProperties = new java.util.HashMap<>();
		kafkaProperties.put("zk.connect", kafkaZk);
		kafkaProperties.put("metadata.broker.list", brokerList);
		Config config = prepareClusterConfig(kafkaProperties);

		LocalCluster cluster = new LocalCluster();
		String topologyName = appProps.getTopologyName();
		cluster.submitTopology(topologyName, config, stormTopology);
		
		Thread.sleep(appProps.getThreadSleep());
		
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

	private Config prepareClusterConfig(Map<String, String> kafkaProperties) {
		Config config = new Config();
		config.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1);
		config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, kafkaProperties);
		config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
		return config;
	}
}
