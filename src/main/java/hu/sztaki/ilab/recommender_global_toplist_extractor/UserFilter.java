package hu.sztaki.ilab.recommender_global_toplist_extractor;

import java.util.Collection;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

public class UserFilter extends
		RichFilterFunction<Tuple3<Long, Double, double[]>> {
	private double lower_bound;
	private Tuple3<Long, Double, double[]> max_user = new Tuple3<Long, Double, double[]>();

	@Override
	public void open(Configuration parameters) throws Exception {
		Collection<Tuple4<Long, Long, Double, Integer>> lower_bound_wrapped = getRuntimeContext()
				.getBroadcastVariable("lower_bound");
		Collection<Tuple3<Long, Double, double[]>> max_user_wrapped = getRuntimeContext()
				.getBroadcastVariable("max_user");
		for (Tuple4<Long, Long, Double, Integer> item : lower_bound_wrapped) {
			this.lower_bound = item.f2;
		}
		for (Tuple3<Long, Double, double[]> item : max_user_wrapped) {
			this.max_user = item;
		}
	}

	@Override
	public boolean filter(Tuple3<Long, Double, double[]> val) throws Exception {
		boolean enabled_in_next_iteration = false;
		if(val.f0 != max_user.f0) { // current user is filtered out 
			enabled_in_next_iteration = (val.f1 > lower_bound) ? true : false;
		}
		return enabled_in_next_iteration;
	}
}
