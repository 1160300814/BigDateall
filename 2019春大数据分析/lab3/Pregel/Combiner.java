package Pregel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class Combiner {
	//public Map<Vertex, Set<Double>> messageQueue = new HashMap<Vertex, Set<Double>>();
    public abstract Map<Vertex,Set<Double>> combine(Map<Vertex,Set<Double>> hh);
}
