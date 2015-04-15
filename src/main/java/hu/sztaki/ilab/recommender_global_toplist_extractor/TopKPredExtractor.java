package hu.sztaki.ilab.recommender_global_toplist_extractor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class TopKPredExtractor implements FlatMapFunction<IterationData, Tuple4<Long, Long, Double, Integer>> {

	@Override
	public void flatMap(IterationData value,
			Collector<Tuple4<Long, Long, Double, Integer>> out)
			throws Exception {
		if(value.isTopKPred()) {
			out.collect(value.getTopKPred());
		}	
	}
}
