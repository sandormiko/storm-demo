package hu.sm.storm.base;

public class AppConstants {

	public static final String KAFKA_MESSAGE = "kafkaMessage";
	public static final String TOPIC = "topic";
	public static String RANDOM_TOPIC_COUNTER_MAP = "randomTopicCounterByTopicName";
	public static String ROOT_TOPIC_COUNTER_MAP = "rootTopicCountByTopicName";
	public static final String BYTES_SENT = "bytesSent";
	public static final String KAFKA_MSG_EXTRACTOR = "kafka-msg-extractor";
	public static final String KAFKA_MSG_SENDER = "kafka-msg-sender";
	public static final String KAFKA_MSG_AGGREGATOR = "kafka-msg-aggregator";
	public static final String ZK_HOST = "zk.host";
	public static final String ZK_PORT = "zk.port";
	public static final String ROOT_TOPIC = "root.topic";
	public static final String ZK_URL = "zk.url";
	public static final String TARGET_TOPIC_1 = "target.topic.1";
	public static final String TARGET_TOPIC_2 = "target.topic.2";
	public static final String TARGET_TOPIC_3 = "target.topic.3";
	public static final String BROKER_PROPERTIES = "broker.properties";
	public static final String TOPOLOGY_NAME = "topology.name";
	public static final String THREAD_SLEEP = "thread.sleep";
	public static final String TIME_BUCKET_SIZE = "timebucket.size";
	public static final Long DEF_THREAD_SLEEP = 60000l;
	public static final int DEF_TIMEBUCKET_SIZE = 50;
}
