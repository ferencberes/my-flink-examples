package hu.sztaki.ilab.recommender_global_toplist_extractor;

import java.util.Collection;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class UpperBoundExtractor extends
		RichMapFunction<Tuple2<Long, double[]>, Tuple3<Long, Double, double[]>> {

	private int feature_num;
	private double[] lower_bounds;
	private double[] upper_bounds;
	private Tuple3<Long, Double, double[]> output = new Tuple3<Long, Double, double[]>();

	public UpperBoundExtractor(int feature_num) {
		this.feature_num = feature_num;
		this.lower_bounds = new double[this.feature_num];
		this.upper_bounds = new double[this.feature_num];
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Collection<Tuple2<Integer, Double>> lower_set = getRuntimeContext()
				.getBroadcastVariable("lower");
		Collection<Tuple2<Integer, Double>> upper_set = getRuntimeContext()
				.getBroadcastVariable("upper");
		for (Tuple2<Integer, Double> item : lower_set) {
			lower_bounds[item.f0] = item.f1;
		}
		for (Tuple2<Integer, Double> item : upper_set) {
			upper_bounds[item.f0] = item.f1;
		}
	}

	@Override
	public Tuple3<Long, Double, double[]> map(Tuple2<Long, double[]> value)
			throws Exception {
		double pred_upper_bound = 0.0;
		double[] factors = value.f1;
		for (int i = 0; i < feature_num; i++) {
			if (factors[i] > 0) {
				pred_upper_bound += (upper_bounds[i] > 0 ? factors[i]
						* upper_bounds[i] : 0);
			} else {
				pred_upper_bound += (lower_bounds[i] < 0 ? factors[i]
						* lower_bounds[i] : 0);
			}
		}
		output.setFields(value.f0, pred_upper_bound, value.f1);
		return output;
	}

};
