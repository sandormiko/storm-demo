package hu.sm.storm.topology.metric;

import java.math.BigDecimal;
import java.math.RoundingMode;

import backtype.storm.metric.api.IMetric;

public class RecordsPerSecondsMetric implements IMetric {
	private double recordsCounter = 0.0;
	private double millisecondsCounter = 0.0;

	public void incrRecords() {
		recordsCounter += Double.valueOf(1);
	}

	public void incrMilliseconds(long ms) {
		millisecondsCounter += Double.valueOf(ms);
	}

	@Override
	public Object getValueAndReset() {
		Double result = new Double(0.0);
		if (recordsCounter != 0.0 && millisecondsCounter != 0.0) {
			result = (recordsCounter / millisecondsCounter) * 1000;
			result = to2FractionDigits(result);
		}

		recordsCounter = 0;
		millisecondsCounter = 0;
		return result;
	}

	private double to2FractionDigits(Double value) {
		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(2, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
}
