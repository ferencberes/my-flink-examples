package hu.sztaki.ilab.recommender_global_toplist_extractor;

import java.util.Comparator;

import org.apache.flink.api.java.tuple.Tuple4;

public class PredictionComparator implements Comparator<Tuple4<Long, Long, Double, Integer>> {
	
	@Override
	public int compare(Tuple4<Long, Long, Double, Integer> o1,
			Tuple4<Long, Long, Double, Integer> o2) {
		if(o1.f2<o2.f2) {
			return -1;
		} else if (o1.f2<o2.f2) {
			return 0;
		} else {
			return 1;
		}
	}
}
