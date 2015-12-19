package hu.sm.storm.topology.metric;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BytesPerSecondsMetricsTest {

	private BytesPerSecondsMetric metric;

	@Before
	public void init() {
		metric = new BytesPerSecondsMetric();
	}

	@Test
	public void shouldCalculateBytesPerSeconds() {
		metric.incrBytes(1);
		metric.incrMilliseconds(1);
		Assert.assertEquals(1000.0, metric.getValueAndReset());
	}

	@Test
	public void shouldReturnZeroBytesPerSeconds() {
		Assert.assertEquals(0.0, metric.getValueAndReset());
	}
}
