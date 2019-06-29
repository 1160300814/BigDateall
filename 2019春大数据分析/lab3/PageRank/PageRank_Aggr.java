package PageRank;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import Pregel.Aggregator;
import Pregel.Vertex;

public class PageRank_Aggr extends Aggregator{

	@Override
	public Map<Vertex, Set<Double>> Aggreg(Vertex v, Set<Double> mm) {
		Map<Vertex, Set<Double>> hh = new HashMap<>();
		hh.put(v, mm);
		return hh;
	}

}
