package PageRank;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import Pregel.Vertex;

public class PageRank_Vertex extends Vertex {
	private static  PageRank_Aggr allpoints = new PageRank_Aggr();
	private int N = 0;

	public PageRank_Vertex(int num, double str) {
		super(num, str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void Compute(Set<Double> str) {
		if(this.GetSteps()>=1 && this.GetSteps()<5) {
			Iterator<Double> it = str.iterator();
			double sum = 0;
			while (it.hasNext()) {
				double outV = it.next();
				sum += outV;
			}
			int N = this.GetOutEdge().size();
			this.ChangeVertexValue(0.15 / N + 0.85 * sum);
		}else if(this.GetSteps()== 0){
			Iterator<Double> it = str.iterator();
			double sum = 0;
			while (it.hasNext()) {
				double outV = it.next();
				sum += outV;
				allpoints.Aggreg(this, str);
			}
			int N = allpoints.Aggreg(this ,str).keySet().size();
			this.ChangeVertexValue(0.15 / N + 0.85 * sum);
		}else {
			this.VoteToHalt();
		}
		return;

	}

}
