package hu.sztaki.ilab.recommender_global_toplist_extractor;

import java.util.Collection;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class PartitionTopKExtractor
		extends
		RichGroupReduceFunction<Tuple3<Long, Double, double[]>, Tuple4<Long, Long, Double, Integer>> {
	private int top_k;
	private int feature_num;
	private long max_user_id;
	private double[] max_user_factors;
	private double lower_bound;
	private PriorityQueue<Tuple4<Long, Long, Double, Integer>> minHeap;

	public PartitionTopKExtractor(int top_k, int feature_num) {
		this.top_k = top_k;
		this.feature_num = feature_num;
		this.minHeap = new PriorityQueue<Tuple4<Long, Long, Double, Integer>>(
				this.top_k/*, new PredictionComparator()*/); // comparator is not serializiable!!!
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Collection<Tuple3<Long, Double, double[]>> max_user_wrapped = getRuntimeContext()
				.getBroadcastVariable("max_user");
		Collection<Tuple4<Long, Long, Double, Integer>> lower_bound_wrapped = getRuntimeContext()
				.getBroadcastVariable("lower_bound");
		for (Tuple3<Long, Double, double[]> item : max_user_wrapped) {
			this.max_user_factors = item.f2;
			this.max_user_id = item.f0;
		}
		for (Tuple4<Long, Long, Double, Integer> item : lower_bound_wrapped) {
			this.lower_bound = item.f2;
		}
	}

	@Override
	public void reduce(Iterable<Tuple3<Long, Double, double[]>> items,
			Collector<Tuple4<Long, Long, Double, Integer>> out)
			throws Exception {
		minHeap.clear();
		for (Tuple3<Long, Double, double[]> element : items) {
			if (element.f1 > lower_bound && element.f1 > minHeap.peek().f2) {
				double sum = 0.0;
				for (int i = 0; i < feature_num; i++) {
					sum += element.f2[i] * max_user_factors[i];
				}
				Tuple4<Long, Long, Double, Integer> for_insert = new Tuple4<Long, Long, Double, Integer>(
						max_user_id, element.f0, sum, 0);
				if (minHeap.size() < top_k) {
					minHeap.offer(for_insert);
				} else {
					if (sum > minHeap.peek().f2) {
						minHeap.poll();
						minHeap.offer(for_insert);
					}
				}
			}
		}
		for (Tuple4<Long, Long, Double, Integer> partial_top_pred : minHeap) {
			out.collect(partial_top_pred);
		}
	}
};
