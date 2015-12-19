package hu.sm.storm.topology.hook;

import java.util.Map;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.task.TopologyContext;
import hu.sm.storm.base.ApplicationProperties;
import hu.sm.storm.topology.metric.BytesPerSecondsMetric;
import hu.sm.storm.topology.metric.RecordsPerSecondsMetric;

public abstract class BaseMetricsHook extends BaseTaskHook {

	private static final String RECORDS_PER_SECONDS = "records-per-seconds";
	private static final String BYTES_PER_SECONDS = "bytes-per-seconds";

	protected transient RecordsPerSecondsMetric recordsPerSeconds = null;
	protected transient BytesPerSecondsMetric bytesPerSeconds = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		ApplicationProperties props = ApplicationProperties.getInstance();
		Integer timeBucketSizeInSecs = props.getTimeBucketSize();
		if (context.getRegisteredMetricByName(RECORDS_PER_SECONDS) == null) {
			recordsPerSeconds = new RecordsPerSecondsMetric();
			context.registerMetric(RECORDS_PER_SECONDS, recordsPerSeconds, timeBucketSizeInSecs);
		}
		if (context.getRegisteredMetricByName(BYTES_PER_SECONDS) == null) {
			bytesPerSeconds = new BytesPerSecondsMetric();
			context.registerMetric(BYTES_PER_SECONDS, bytesPerSeconds, timeBucketSizeInSecs);
		}
	}
}
