package PageRank;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import Pregel.Combiner;
import Pregel.Master;
import Pregel.Vertex;

public class PageRank {

	private static Set<Vertex> allpoints = new HashSet<>();

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
			Vertex from = new PageRank_Vertex(Integer.parseInt(spl[0]), Double.POSITIVE_INFINITY);
			if (!allpoints.contains(from)) {
				allpoints.add(from);
			}
			// 判断点中是否含顶点to
			// numbers++;
			Vertex to = new PageRank_Vertex(Integer.parseInt(spl[spl.length - 1]), Double.POSITIVE_INFINITY);
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
		Combiner cc = new PageRank_Combiner();
		Master master = new Master(allpoints, 50, cc);
		master.Run();

	}

}
