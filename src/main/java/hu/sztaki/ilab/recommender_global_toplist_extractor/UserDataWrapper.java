package hu.sztaki.ilab.recommender_global_toplist_extractor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class UserDataWrapper implements MapFunction<Tuple3<Long, Double, double[]>, IterationData> {
	private IterationData output = new IterationData();
	
	@Override
	public IterationData map(Tuple3<Long, Double, double[]> value)
			throws Exception {
		output.setField(1,0); // set identifier for UserData
		output.setField(value, 1);
		return output;
	}

}
