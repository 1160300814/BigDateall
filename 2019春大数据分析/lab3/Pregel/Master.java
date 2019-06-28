package Pregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Master {
	private List<Worker> MyWorkers = new ArrayList<Worker>();// Worker列表
	private int WorksNum = 1;// 分几个worksWorks
	private List<Set<Vertex>> Partall = new ArrayList<>();// 顶点分组结果
	private Set<Vertex> graphall;// 全图==全电
	private int Maststeps = -1;
	private Combiner CC;

	public Master(Set<Vertex> gp, int wnum, Combiner cc) {
		this.graphall = gp;
		this.WorksNum = wnum;
		this.CC = cc;
	}

	public void Run() {
		this.Partall = this.Partition();
		int ActiveWorksNum = this.WorksNum;
		if (Partall.size() != ActiveWorksNum) {// 检验
			System.out.println("Master中Wrong：Partall.size() != ActiveWorksNum ! ");
			return;
		}

		for (int i = 0; i < ActiveWorksNum; i++) {// 初始化worker
			Worker work = new Worker(i, Partall.get(i), CC);
			System.out.println("Work: " + i + "\t Vertex: " + Partall.get(i).size() + "\t");// 3.10統計器
			long Esum = 0;
			for (Vertex EK : Partall.get(i)) {
				Esum += EK.GetOutEdge().keySet().size();
			}
			System.out.println("Edge: " + Esum);// 3.10統計器

			this.MyWorkers.add(work);
		}

		// boolean state = true;
		int getworkerback = 0;
		long SendM = 0;
		long GetM = 0;
		boolean isnextactiveworker = true;
		while (isnextactiveworker) {// 开始一轮BSP
			Maststeps++;
			isnextactiveworker = false;
			for (int i = 0; i < ActiveWorksNum; i++) {
				Worker work = this.MyWorkers.get(i);
				long Wstart = System.currentTimeMillis();// 一个WORKER开始时间
				boolean workrea = work.StartRun();
				long Wend = System.currentTimeMillis();// 一个WORKER结束时间
				System.out.println("BSP: " + isnextactiveworker + "\t Worker： " + i + "\t Total Time:");// 3.10統計器
				System.out.println(Wend - Wstart);// 3.10統計器
				// 统计works完成数
				if (workrea) {// n
					getworkerback++;
					for (Set<Double> SN : work.Sendsessage.values()) {
						SendM += SN.size();
					}
					for (Set<Double> GN : work.NowMesseges.values()) {
						GetM += GN.size();
					}
					System.out.println("Send: " + SendM + "\t Get： " + GetM + "\t Total Message: " + (GetM + SendM));// 3.10統計器

				}
				// 判断下一轮仍有活跃worker，BSP未停
				if (work.GetWordFlag()) {
					isnextactiveworker = true;
				}
			}

			if (getworkerback != this.WorksNum) {// 判断是否有worker未完成
				System.out.println("BSP: " + isnextactiveworker + "\t 未完成Worker： " + (this.WorksNum - getworkerback));
			}

		}

	}

	/**
	 * 将全部点 随机分组
	 * 
	 * @param graph @return[（点）（点）][（点）（点）][......]
	 */
	public List<Set<Vertex>> Partition() {
		List<Set<Vertex>> part = new ArrayList<>();
		Set<Vertex> gk = graphall;// 图的起始点
		Set<Vertex> tmp = new HashSet<>();
		Map<Integer, Set<Vertex>> hold = new HashMap<>();// 起始点随机化分组
		// 将点随机分配到不同的分组（随机值，点集）
		for (Vertex key : gk) {
			int rd = (int) Math.random() * WorksNum;// 随机化
			if (!hold.containsKey(rd))
				hold.put(rd, new HashSet<Vertex>());
			hold.get(rd).add(key);
		}
		// 将同一组的点整合[（点）（点）][（点）（点）][......
		for (Integer key : hold.keySet()) {// 同一组
			tmp = new HashSet<>();
			for (Vertex item : hold.get(key)) {// 遍历同一组中的点
				tmp.add(item);// 同一组的【（点）（点）（点）......】
			}
			part.add(tmp);
		}
		return part;
	}

}
