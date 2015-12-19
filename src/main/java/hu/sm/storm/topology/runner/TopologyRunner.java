package hu.sm.storm.topology.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sm.storm.base.ApplicationProperties;
import hu.sm.storm.topology.submitter.TopologySubmitter;

public class TopologyRunner {

	private static final String C_NAME = TopologyRunner.class.getName();
	private static Logger LOG = LoggerFactory.getLogger(TopologyRunner.class);

	public static void main(String[] args) throws InterruptedException{
		if (LOG.isDebugEnabled()) {
			LOG.debug("Entering " + C_NAME + ".main");
		}
		try {
			initializeAppProps(args);
			TopologySubmitter bean = new TopologySubmitter();
			bean.submitTopology();
		}catch(Exception e) {
			handleException(e);
		}
		finally {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Exiting " + C_NAME + ".execute");
			}
		}
	}

	private static void initializeAppProps(String[] args) {
		if(args.length < 1){
			System.out.println("Path to application properties file must be given");
		}
		String filePath = args[0];
		ApplicationProperties appProps = ApplicationProperties.getInstance();
		appProps.initialize(filePath);
	}

	private static void handleException(Exception ex){
		ex.printStackTrace(System.err);
		System.exit(1);
	}
}
