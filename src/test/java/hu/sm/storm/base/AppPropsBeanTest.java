package hu.sm.storm.base;

import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AppPropsBeanTest {

	private static ApplicationProperties appPropsBean = null;
	
	@BeforeClass
	public static void staticInit(){
		Properties props = new Properties();
		props.put(AppConstants.ROOT_TOPIC, "root.topic");
		props.put(AppConstants.TARGET_TOPIC_1, "topic.1");
		props.put(AppConstants.TARGET_TOPIC_2, "topic.2");
		props.put(AppConstants.TARGET_TOPIC_3, "topic.3");
		props.put(AppConstants.TOPOLOGY_NAME, "topology.name");
		props.put(AppConstants.ZK_URL, "test:2181");
		props.put(AppConstants.BROKER_PROPERTIES, "broker:2181");
		props.put(AppConstants.BROKER_PROPERTIES, "broker:2181");
		props.put(AppConstants.THREAD_SLEEP, "123456");
		props.put(AppConstants.TIME_BUCKET_SIZE, "11");
		appPropsBean = ApplicationProperties.getInstance();
		appPropsBean.initialize(props);
	}
	
	@Test
	public void testgetRootTopic(){
		Assert.assertEquals("root.topic", appPropsBean.getRootTopic());
	}
	
	@Test
	public void testGetZkUrl(){
		Assert.assertEquals("test:2181", appPropsBean.getZookeeperUrl());
	}
	
	@Test
	public void testGetTargetTopic1(){
		Assert.assertEquals("topic.1", appPropsBean.getTargetTopic1());
	}
	
	@Test
	public void testGetTargetTopic2(){
		Assert.assertEquals("topic.2", appPropsBean.getTargetTopic2());
	}
	@Test
	public void testGetTargetTopic3(){
		Assert.assertEquals("topic.3", appPropsBean.getTargetTopic3());
	}
	
	@Test
	public void testGetTopologyName(){
		Assert.assertEquals("topology.name", appPropsBean.getTopologyName());
	}
	
	@Test
	public void testGetBrokerProperties(){
		Assert.assertEquals("broker:2181", appPropsBean.getBrokerProperties());
	}
	
	@Test
	public void testGetThreadSleep(){
		Assert.assertEquals(new Long(123456), appPropsBean.getThreadSleep());
	}
	
	@Test
	public void testGetTimeBucketSize(){
		Assert.assertEquals(new Integer(11), appPropsBean.getTimeBucketSize());
	}
	
}
