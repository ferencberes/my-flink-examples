package hu.sztaki.ilab.recommender_global_toplist_extractor;

import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FactorRandomGenerator implements
		MapFunction<Long, Tuple2<Long, double[]>> {
	private Tuple2<Long, double[]> output = new Tuple2<Long, double[]>();
	private int feature_num;

	public FactorRandomGenerator(int feature_num) {
		this.feature_num = feature_num;
	}

	@Override
	public Tuple2<Long, double[]> map(Long value) throws Exception {
		Random rnd = new Random();
		double[] factors = new double[feature_num];
		for (int i = 0; i < feature_num; i++) {
			factors[i] = rnd.nextDouble();
		}
		output.setFields(value, factors);
		return output;
	}
};
