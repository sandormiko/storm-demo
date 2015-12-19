package hu.sm.storm.topology.hook;

import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.tuple.Tuple;
import hu.sm.storm.base.AppConstants;
import hu.sm.storm.base.domain.KafkaMessage;

public class BoltMetricsHook extends BaseMetricsHook {

	@Override
	public void boltExecute(BoltExecuteInfo info) {
		recordsPerSeconds.incrMilliseconds(info.executeLatencyMs);
		bytesPerSeconds.incrMilliseconds(info.executeLatencyMs);
		recordsPerSeconds.incrRecords();
		collectByteInfo(info.tuple);
	}

	private void collectByteInfo(Tuple input) {
		if (input.contains(AppConstants.KAFKA_MESSAGE)) {
			KafkaMessage msg = (KafkaMessage) input.getValueByField(AppConstants.KAFKA_MESSAGE);
			bytesPerSeconds.incrBytes(msg.getSizeInBytes());
		} else if (input.getValues().get(0) instanceof byte[]) {
			byte[] data = input.getBinary(0);
			bytesPerSeconds.incrBytes(data.length);
		}
	}
}
