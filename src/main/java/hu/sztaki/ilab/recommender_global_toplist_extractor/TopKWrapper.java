package hu.sztaki.ilab.recommender_global_toplist_extractor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class TopKWrapper implements MapFunction<Tuple4<Long, Long, Double, Integer>, IterationData> {
	private IterationData output = new IterationData();
	
	@Override
	public IterationData map(Tuple4<Long, Long, Double, Integer> value)
			throws Exception {
		output.setField(0, 0); // set identifier for topKPred
		output.setField(value, 2);
		return output;
	}
}
