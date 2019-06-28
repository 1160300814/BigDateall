package Pregel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Worker {
	private Set<Vertex> MyVertexs = new HashSet<Vertex>();// 负责的顶点
	private Map<Vertex, Set<Double>> LastMesseges = new HashMap<Vertex, Set<Double>>();// 保存上一轮接收的消息
	public Map<Vertex, Set<Double>> NowMesseges = new HashMap<Vertex, Set<Double>>();// 保存当前轮接收的消息
	public Map<Vertex,Set<Double>> Sendsessage = new HashMap<Vertex,Set<Double>>();
	// Set<Vertex>>();// 保存相应出边
	private boolean WorkFlag = true;// 活跃
	private Map<Vertex, Set<Double>> AnswerCombine = new HashMap<Vertex, Set<Double>>();
	Combiner combine;
	private int WorkSteps = -1;
	private int WorkID;

	public Worker(int num, Set<Vertex> vers, Combiner cc) {
		this.WorkID = num;
		this.MyVertexs = vers;
		this.combine = cc;
	}

	/**
	 * 开始一轮
	 * 
	 * @return 工作结束返回true
	 */
	public boolean StartRun() {
		WorkSteps++;
		if (WorkSteps == 0) {// 0轮
			Iterator<Vertex> allworks = this.MyVertexs.iterator();
			while (allworks.hasNext()) {
				Vertex myV = allworks.next();
				if (myV.GetSteps() == -1) {// 0轮
					myV.SendMessageTo();
				} else {
					System.out.println("Works Wrong");
					return true;
				}
			}

		} else {// 非0轮

			this.LastMesseges = this.NowMesseges;

			if (this.LastMesseges.isEmpty() || (!WorkFlag)) {// 上一轮未接收到消息，本轮为不活跃worker
				System.out.println(
						"此Worker不活跃, Worker结束工作 (LastMesseges: " + this.LastMesseges.isEmpty() + "\t Worker活跃标志 ： " + WorkFlag+" )");
				return true;
			}

			// 循环本轮活跃顶点，执行compute
			Iterator<Entry<Vertex, Set<Double>>> ActiveWor = this.LastMesseges.entrySet().iterator();
			while (ActiveWor.hasNext()) {
				Map.Entry<Vertex, Set<Double>> entry = ActiveWor.next();
				entry.getKey().Compute(entry.getValue());// 执行compute
				if(!Sendsessage.keySet().contains(entry.getKey())) {
					Set<Double> kkkk = new HashSet<>();
					kkkk.add(entry.getKey().GetVertexValue());
					Sendsessage.put(entry.getKey(), kkkk);//发送消息队列
				}else {
					Sendsessage.get(entry.getKey()).add(entry.getKey().GetVertexValue());//发送消息队列
				}
				entry.getKey().SendMessageTo();
			}
		}

		// 统计当前轮所有顶点接受的消息
		this.NowMesseges.clear();
		Iterator<Vertex> newWorks = this.MyVertexs.iterator();
		while (newWorks.hasNext()) {
			Vertex outV = newWorks.next();
			NowMesseges.put(outV, outV.GetInmessege());
			outV.ClearInmessege();// 清除
		}
		this.NowMesseges = combine.combine(this.NowMesseges);//进入接受队列进行Combine

		// 判断本轮结束后仍处于活跃的顶点
		if (!this.NowMesseges.isEmpty()) {
			WorkFlag = true;// 活跃
		} else {
			WorkFlag = false;
		}

		return true;// 完成工作
	}

	/**
	 * 得到Work本轮结束后是否活跃
	 * 
	 * @return
	 */
	public boolean GetWordFlag() {
		return this.WorkFlag;
	}

}
