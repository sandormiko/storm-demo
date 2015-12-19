package hu.sm.storm.topology.hook;

import java.util.List;

import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;

public class SpoutMetricsHook extends BaseMetricsHook {

	@Override
	public void emit(EmitInfo info) {
		recordsPerSeconds.incrRecords();
		List<Object> values = info.values;
		for (Object val : values) {
			if (val instanceof byte[]) {
				bytesPerSeconds.incrBytes(((byte[]) val).length);
			}
		}
	}

	@Override
	public void spoutAck(SpoutAckInfo info) {
		recordsPerSeconds.incrMilliseconds(info.completeLatencyMs);
		bytesPerSeconds.incrMilliseconds(info.completeLatencyMs);
	}

}
