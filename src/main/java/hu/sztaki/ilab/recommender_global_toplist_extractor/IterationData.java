package hu.sztaki.ilab.recommender_global_toplist_extractor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

// Tuple3<Long, Double, double[]> : represents users <user_id,upper_bound,factors>
// Tuple4<Long, Long, Double, Integer> : represents top_k predictions <user_id,item_id,prediction,default_pos>

public class IterationData extends Tuple3<Integer,Tuple3<Long, Double, double[]>, Tuple4<Long, Long, Double, Integer>> {
	private static double[] arr = new double[]{0.0};
	private static Tuple3<Long, Double, double[]> default_1 = new Tuple3<Long, Double, double[]>(-1L,0.0,arr);
	private static Tuple4<Long, Long, Double, Integer> default_2 = new Tuple4<Long, Long, Double, Integer>(-1L,-1L,0.0,0);
	
	public IterationData()  {
		super();
		this.setFields(-1, default_1, default_2); // null mezőkkel nem működött!!!
	}
	
	public boolean isTopKPred() {
		return this.f0 == 0;
	}
	
	public boolean isUserData() {
		return this.f0 == 1;
	}
	
	public Tuple3<Long, Double, double[]> getUserData() {
		return this.f1;
	}
	
	public Tuple4<Long, Long, Double, Integer> getTopKPred() {
		return this.f2;
	}
}
