package SSSP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import Pregel.Combiner;
import Pregel.Vertex;

public class SSSP_Combiner extends Combiner{

	@Override
	public Map<Vertex, Set<Double>> combine(Map<Vertex, Set<Double>> hh) {
		Map<Vertex, Set<Double>> ans = new HashMap<>();
		while(!hh.isEmpty()){
			Iterator<Entry<Vertex, Set<Double>>> ss = hh.entrySet().iterator();
			while (ss.hasNext()) {
				Map.Entry<Vertex, Set<Double>> entry = ss.next();
				Double minSS = Double.POSITIVE_INFINITY;
				for(Double k:entry.getValue()) {//找到消息队列中最小值
					if(minSS > k) {
						minSS = k;
					}
				}
				Set<Double> MinSS =  new HashSet<Double>();
				MinSS.add(minSS);
				//找到某个顶点的最小值
				if(!ans.containsKey(entry.getKey())){
                    ans.put(entry.getKey(), MinSS);
                }
                else{
                	Set<Double> re =  new HashSet<Double>();
                	re.add(Math.min(minSS, Double.parseDouble(ans.get(entry.getKey()).toString())));
                    ans.put(entry.getKey(), re);
                }
				entry.getKey().Compute(entry.getValue());// 执行compute
			}     
            
        }
		return ans;
	}

}
