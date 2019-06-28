package PageRank;

import java.util.Iterator;
import java.util.Set;

import Pregel.Vertex;

public class PageRank_Vertex extends Vertex {
	private static Set<Vertex> allpoints;
	private int N = allpoints.size();

	public PageRank_Vertex(int num, double str) {
		super(num, str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void Compute(Set<Double> str) {
		Iterator<Double> it = str.iterator();
		double sum = 0;
		while (it.hasNext()) {
			double outV = it.next();
			sum += outV;
		}
		this.ChangeVertexValue(0.15 / N + 0.85 * sum);
		this.VoteToHalt();
		return;

	}

}
