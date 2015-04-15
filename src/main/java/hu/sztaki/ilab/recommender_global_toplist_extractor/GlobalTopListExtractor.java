package hu.sztaki.ilab.recommender_global_toplist_extractor;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class GlobalTopListExtractor {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		if (args.length == 4) {
			int feature_num = Integer.parseInt(args[0]);
			long user_num = Long.parseLong(args[1]);
			long item_num = Long.parseLong(args[2]);
			int top_k = Integer.parseInt(args[3]);

			DataSet<Long> user_id_list = env.generateSequence(0, user_num)
					.name("Generate user ids");
			DataSet<Long> item_id_list = env.generateSequence(0, item_num)
					.name("Generate item ids");

			DataSet<Tuple2<Long, double[]>> user_factors = user_id_list.map(
					new FactorRandomGenerator(feature_num)).name(
					"Generate random user factors");

			DataSet<Tuple2<Long, double[]>> item_factors = item_id_list.map(
					new FactorRandomGenerator(feature_num)).name(
					"Generate random item factors");

			DataSet<Tuple2<Integer, Double>> user_entities = user_factors
					.flatMap(new EntityMapper());
			DataSet<Tuple2<Integer, Double>> item_entities = item_factors
					.flatMap(new EntityMapper());

			DataSet<Tuple2<Integer, Double>> user_entity_upper = user_entities
					.groupBy(0).max(1);
			DataSet<Tuple2<Integer, Double>> user_entity_lower = user_entities
					.groupBy(0).min(1);
			DataSet<Tuple2<Integer, Double>> item_entity_upper = item_entities
					.groupBy(0).max(1);
			DataSet<Tuple2<Integer, Double>> item_entity_lower = item_entities
					.groupBy(0).min(1);

			// user_entity_lower.print();
			// user_entity_upper.print();
			// item_entity_lower.print();
			// item_entity_upper.print();

			DataSet<Tuple3<Long, Double, double[]>> user_factors_with_bound = user_factors
					.map(new UpperBoundExtractor(feature_num))
					.withBroadcastSet(item_entity_upper, "upper")
					.withBroadcastSet(item_entity_lower, "lower")
					.name("Extracting upper bounds for users");

			DataSet<Tuple3<Long, Double, double[]>> item_factors_with_bound = item_factors
					.map(new UpperBoundExtractor(feature_num))
					.withBroadcastSet(user_entity_upper, "upper")
					.withBroadcastSet(user_entity_lower, "lower")
					.name("Extracting upper bounds for items");

			// user_factors_with_bound.print();
			// item_factors_with_bound.print();

			LinkedList<Tuple4<Long, Long, Double, Integer>> list = new LinkedList<Tuple4<Long, Long, Double, Integer>>();
			Tuple4<Long, Long, Double, Integer> default_prediction = new Tuple4<Long, Long, Double, Integer>(
					-1L, -1L, -Double.MAX_VALUE, 0);
			list.add(default_prediction);

			DataSet<Tuple4<Long, Long, Double, Integer>> initial_top_k = env
					.fromCollection((Collection<Tuple4<Long, Long, Double, Integer>>) list);
			DataSet<IterationData> first_top_k = initial_top_k
					.map(new TopKWrapper());
			DataSet<IterationData> first_user_data = user_factors_with_bound
					.map(new UserDataWrapper());

			DataSet<IterationData> initial_data = first_top_k
					.union(first_user_data);

			initial_data.print();

			IterativeDataSet<IterationData> iter_data = initial_data
					.iterate((int) user_num);

			DataSet<Tuple4<Long, Long, Double, Integer>> prev_top_k = iter_data
					.flatMap(new TopKPredExtractor());
			DataSet<Tuple3<Long, Double, double[]>> prev_users = iter_data
					.flatMap(new UserDataExtractor());

			DataSet<Tuple4<Long, Long, Double, Integer>> min_element_1 = prev_top_k
					.min(2).name("Extracting top_k lower bound");

			DataSet<Tuple3<Long, Double, double[]>> max_user = prev_users
					.max(1).name("Get user with max upper bound");

			// TODO: there is some null pointer exception here!!!
			DataSet<Tuple4<Long, Long, Double, Integer>> top_k_partitions = item_factors_with_bound
					.reduceGroup(new PartitionTopKExtractor(top_k, feature_num))
					.withBroadcastSet(max_user, "max_user")
					.withBroadcastSet(min_element_1, "lower_bound")
					.name("Extract top_k for each partition");

			// Note: group by on first id OK bc. this is top_k for only one user
			DataSet<Tuple4<Long, Long, Double, Integer>> top_k_for_max_user = top_k_partitions
					.groupBy(0).sortGroup(2, Order.DESCENDING).first(top_k)
					.name("Extract top_k for max user");

			// TODO: first line is the original: it is only temporary solution
			// because of some bug!
			// DataSet<Tuple4<Long, Long, Double, Integer>> next_top_k =
			// top_k_for_max_user
			DataSet<Tuple4<Long, Long, Double, Integer>> next_top_k = prev_top_k
					.union(prev_top_k).groupBy(3)
					.sortGroup(2, Order.DESCENDING).first(top_k)
					.name("Update global top_k");

			// TODO: az iter data-ba be kell csomagolni a min elemét a global
			// toplist-nek, hogy ne kelljen x2 minimumot keresni!!!
			// vagy valami iteráció változóba elmenteni!!!
			DataSet<Tuple4<Long, Long, Double, Integer>> min_element2 = next_top_k
					.min(2).name("Extracting top_k lower bound");

			DataSet<Tuple3<Long, Double, double[]>> next_user_data = prev_users
					.filter(new UserFilter())
					.withBroadcastSet(min_element2, "lower_bound")
					.withBroadcastSet(max_user, "max_user")
					.name("Filter users for next iteration");

			DataSet<IterationData> wrapped_next_top_k = next_top_k
					.map(new TopKWrapper());
			DataSet<IterationData> wrapped_next_user_data = next_user_data
					.map(new UserDataWrapper());

			DataSet<IterationData> next_iter_data = wrapped_next_top_k
					.union(wrapped_next_user_data);

			// termination criteria: there is no UserData left in the iteration
			// dataset.
			first_top_k = iter_data.closeWith(next_iter_data,
					next_iter_data.flatMap(new UserDataExtractor()));

			DataSet<Tuple3<Long, Long, Double>> global_top_k = first_top_k
					.flatMap(new TopKPredExtractor()).project(0, 1, 2);
			global_top_k.print();

			env.execute("GlobalTopListExtractor");
			// System.out.println(env.getExecutionPlan());

		} else {
			System.out
					.println("Usage: <feature_num> <user_num> <item_num> <top_k>");
		}
	}
}
