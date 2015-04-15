package hu.sztaki.ilab.recommender_global_toplist_extractor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class UserDataExtractor implements FlatMapFunction<IterationData, Tuple3<Long, Double, double[]>> {

	@Override
	public void flatMap(IterationData value,
			Collector<Tuple3<Long, Double, double[]>> out) throws Exception {
		if(value.isUserData()) {
			out.collect(value.getUserData());
		}
	}
}
