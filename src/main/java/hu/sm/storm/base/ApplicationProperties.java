package hu.sm.storm.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ApplicationProperties {

	private static Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);
	private static ApplicationProperties instance = null;

	private Properties appProps = null;

	public static ApplicationProperties getInstance() {
		if (instance == null) {
			synchronized (ApplicationProperties.class) {
				if (instance == null) {
					instance = new ApplicationProperties();
				}
			}
		}
		return instance;
	}

	public String getZkHost() {
		return appProps.getProperty(AppConstants.ZK_HOST);
	}

	public String getZkPort() {
		return appProps.getProperty(AppConstants.ZK_PORT);
	}

	public String getZookeeperUrl() {
		return appProps.getProperty(AppConstants.ZK_URL);
	}

	public String getRootTopic() {
		return appProps.getProperty(AppConstants.ROOT_TOPIC);
	}

	public String getTargetTopic1() {
		return appProps.getProperty(AppConstants.TARGET_TOPIC_1);
	}

	public String getTargetTopic2() {
		return appProps.getProperty(AppConstants.TARGET_TOPIC_2);
	}

	public String getTargetTopic3() {
		return appProps.getProperty(AppConstants.TARGET_TOPIC_3);
	}

	public String getBrokerProperties() {
		return appProps.getProperty(AppConstants.BROKER_PROPERTIES);
	}

	public String getTopologyName() {
		return appProps.getProperty(AppConstants.TOPOLOGY_NAME);
	}

	public Long getThreadSleep() {
		String tsAsString = appProps.getProperty(AppConstants.THREAD_SLEEP);
		if (NumberUtils.isNumber(tsAsString)) {
			return Long.valueOf(tsAsString);
		}

		return AppConstants.DEF_THREAD_SLEEP;
	}

	public Integer getTimeBucketSize() {
		String bucketSizeString = appProps.getProperty(AppConstants.TIME_BUCKET_SIZE);
		if (NumberUtils.isNumber(bucketSizeString)) {
			return Integer.valueOf(bucketSizeString);
		}

		return AppConstants.DEF_TIMEBUCKET_SIZE;
	}

	public void initialize(String filePath) {
		InputStream input = null;
		;
		try {
			input = new FileInputStream(new File(filePath));
			appProps = new Properties();
			appProps.load(input);
		} catch (IOException e) {
			handleException(e);
		} finally {
			IOUtils.closeQuietly(input);
		}

	}

	private void handleException(IOException e) {
		String msg = "Initializing Application properties failed";
		LOG.error(msg, e);
		throw new RuntimeException(msg, e);
	}

	protected void initialize(Properties props) {
		appProps = props;

	}
}
