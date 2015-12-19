package hu.sm.storm.topology.metric;

import java.math.BigDecimal;
import java.math.RoundingMode;

import backtype.storm.metric.api.IMetric;

public class BytesPerSecondsMetric implements IMetric {

	private double bytesCounter;
	private double millisecondsCounter;

	public void incrBytes(long bytes) {
		bytesCounter += Double.valueOf(bytes);
	}

	public void incrMilliseconds(long seconds) {
		millisecondsCounter += Double.valueOf(seconds);
	}

	@Override
	public Object getValueAndReset() {
		Double result = new Double(0.0);
		if (millisecondsCounter > 0) {
			result = (bytesCounter / millisecondsCounter) * 1000;
			return to2FractionDigits(result);
		}
		bytesCounter = 0;
		millisecondsCounter = 0;
		return result;
	}

	private double to2FractionDigits(Double value) {
		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(2, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
}
