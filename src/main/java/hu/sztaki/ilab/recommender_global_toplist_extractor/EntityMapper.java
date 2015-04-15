package hu.sztaki.ilab.recommender_global_toplist_extractor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class EntityMapper implements
		FlatMapFunction<Tuple2<Long, double[]>, Tuple2<Integer, Double>> {

	@Override
	public void flatMap(Tuple2<Long, double[]> value,
			Collector<Tuple2<Integer, Double>> out) throws Exception {
		double[] factors = value.f1;
		int feature_num = factors.length;
		for (int i = 0; i < factors.length; i++) {
			out.collect(new Tuple2<Integer, Double>(i, factors[i]));
		}
	}
};