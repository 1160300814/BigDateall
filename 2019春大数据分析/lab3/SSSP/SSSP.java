package SSSP;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import Pregel.Combiner;
import Pregel.Master;
import Pregel.Vertex;

public class SSSP {
	// private static Map<Vertex, Set<Vertex>> graph = new HashMap<Vertex,
	// Set<Vertex>>();// 存的图，key为边起始点，value为终止点
	private static Set<Vertex> allpoints = new HashSet<>();
	// private static int numbers = -1;

	public static void main(String[] args) throws FileNotFoundException {
		// 读入文件
		Scanner sc = new Scanner(new FileInputStream("./src/web-Google.txt"));
		while (sc.hasNext()) {
			String tmp = sc.nextLine();
			if (tmp.startsWith("#"))
				continue;
			String[] spl = tmp.split("\t");
			// 判断点中是否含顶点from
			// numbers++;
			Vertex from = new SSSP_Vertex(Integer.parseInt(spl[0]), Double.POSITIVE_INFINITY);
			if (!allpoints.contains(from)) {
				if(from.GetID()==0) {//从最小顶点开始
					from.ChangeVertexValue(0);
				}
				allpoints.add(from);
			}
			// 判断点中是否含顶点to
			// numbers++;
			Vertex to = new SSSP_Vertex(Integer.parseInt(spl[spl.length - 1]), Double.POSITIVE_INFINITY);
			if (!allpoints.contains(to)) {
				allpoints.add(to);
			}
			/*
			 * else { numbers--;// 包含计数-1 }
			 */
			// 判断图中起始点是否包含from
			/*
			 * if (!graph.containsKey(from)) { graph.put(from, new HashSet<Vertex>());
			 * 
			 * } graph.get(from).add(to);
			 */
			from.AddOutEdge(to, 1.0);

		}
		sc.close();

		//
		Combiner cc = new SSSP_Combiner();
		Master master = new Master(allpoints, 50, cc);
		master.Run();
		
	}

}
