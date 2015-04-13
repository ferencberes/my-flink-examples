package hu.sztaki.ilab.recommender_global_toplist_extractor;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Random;

public class GlobalTopListExtractor {
	private static int feature_num;
	private static long user_num;
	private static long item_num;
	private static int top_k;
	
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		if(args.length == 4) {
		feature_num = Integer.parseInt(args[0]);
		user_num = Long.parseLong(args[1]);
		item_num = Long.parseLong(args[2]);
		top_k = Integer.parseInt(args[3]);

		DataSet<Long> user_id_list = env.generateSequence(0, user_num);
		DataSet<Long> item_id_list = env.generateSequence(0, item_num);

		user_id_list.print();
		
		DataSet<Tuple2<Long, double[]>> user_factors = user_id_list
				.map(new FactorRandomGenerator());
		DataSet<Tuple2<Long, double[]>> item_factors = item_id_list
				.map(new FactorRandomGenerator());
		DataSet<Tuple3<Long, Long, Double>> predictions = user_factors.cross(
				item_factors).with(new PredictionEvaluator());

		DataSet<Tuple4<Long, Long, Double, Integer>> top_list_for_users = predictions
				.groupBy(0).sortGroup(2, Order.DESCENDING).first(top_k)
				.map(new Appender());

		DataSet<Tuple3<Long, Long, Double>> global_toplist = top_list_for_users
				.groupBy(3).sortGroup(2, Order.DESCENDING).first(top_k)
				.project(0, 1, 2);

		global_toplist.print();		
		
		env.execute("GlobalTopListExtractor");
		
		} else {
			System.out.println("Usage: <feature_num> <user_num> <item_num> <top_k>");
		}
	}

	public static final class FactorRandomGenerator implements
			MapFunction<Long, Tuple2<Long, double[]>> {
		public Tuple2<Long, double[]> output = new Tuple2<Long, double[]>();

		@Override
		public Tuple2<Long, double[]> map(Long value) throws Exception {
			Random rnd = new Random();
			double[] factors = new double[feature_num];
			for (int i = 0; i < factors.length; i++) {
				factors[i] = rnd.nextDouble();
			}
			output.setFields(value, factors);
			return output;
		}
	}

	public static final class PredictionEvaluator
			implements
			CrossFunction<Tuple2<Long, double[]>, Tuple2<Long, double[]>, Tuple3<Long, Long, Double>> {
		public Tuple3<Long, Long, Double> output = new Tuple3<Long, Long, Double>();

		@Override
		public Tuple3<Long, Long, Double> cross(
				Tuple2<Long, double[]> user_factor,
				Tuple2<Long, double[]> item_factor) throws Exception {
			double sum = 0.0;
			for (int i = 0; i < feature_num; i++) {
				sum += user_factor.f1[i] * item_factor.f1[i];
			}
			output.setFields(user_factor.f0, item_factor.f0, sum);
			return output;
		}
	}

	public static final class Appender
			implements
			MapFunction<Tuple3<Long, Long, Double>, Tuple4<Long, Long, Double, Integer>> {
		public Tuple4<Long, Long, Double, Integer> output = new Tuple4<Long, Long, Double, Integer>();

		@Override
		public Tuple4<Long, Long, Double, Integer> map(
				Tuple3<Long, Long, Double> value) throws Exception {
			output.setFields(value.f0, value.f1, value.f2, 1);
			return output;
		}

	}
}
