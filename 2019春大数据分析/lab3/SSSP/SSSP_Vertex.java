package SSSP;

import java.util.Iterator;
import java.util.Set;

import Pregel.Vertex;

public class SSSP_Vertex extends Vertex {

	public SSSP_Vertex(int num, Double str) {
		super(num, str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void Compute(Set<Double> str) {
		Iterator<Double> it = str.iterator();
		double smallest = this.GetVertexValue();
		while (it.hasNext()) {
			double outV = it.next();
			if(smallest < (outV+1.0)) {
				smallest = outV+1.0;
			}
		}
		if(this.GetVertexValue() > smallest) {
			this.ChangeVertexValue(smallest);
		}else {//顶点值未发生改变，变为不活跃状态
			this.VoteToHalt();
		}
		this.ChangeVertexValue(smallest);
        return ;

	}
}
