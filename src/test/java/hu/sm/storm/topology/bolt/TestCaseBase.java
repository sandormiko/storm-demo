package hu.sm.storm.topology.bolt;

import backtype.storm.Testing;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import hu.sm.storm.base.AppConstants;
import hu.sm.storm.base.domain.KafkaMessage;

public abstract class TestCaseBase {

	protected Tuple prepareTuple(){
		KafkaMessage message = new KafkaMessage(1, 2, "Data", 1);
		MkTupleParam multiparam = new MkTupleParam();
		multiparam.setFields(AppConstants.KAFKA_MESSAGE, AppConstants.TOPIC);
		return Testing.testTuple(new Values(message, "random1"), multiparam);
	}
}
