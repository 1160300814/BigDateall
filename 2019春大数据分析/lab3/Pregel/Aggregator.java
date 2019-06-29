package Pregel;

import java.util.Map;
import java.util.Set;

public abstract class Aggregator {
	public abstract Map<Vertex,Set<Double>> Aggreg(Vertex v,Set<Double> mm);
}
