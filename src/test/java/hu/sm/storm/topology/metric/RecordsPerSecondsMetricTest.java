package hu.sm.storm.topology.metric;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RecordsPerSecondsMetricTest {

	private RecordsPerSecondsMetric metric;

	@Before
	public void init() {
		metric = new RecordsPerSecondsMetric();
	}

	@Test
	public void shouldCalculateRecordsPerSeconds() {
		metric.incrRecords();
		metric.incrMilliseconds(1);
		Assert.assertEquals(1000.0, metric.getValueAndReset());
	}

	@Test
	public void shouldReturnZero() {
		Assert.assertEquals(0.0, metric.getValueAndReset());
	}
}
