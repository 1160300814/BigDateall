package Pregel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Worker {
	private Set<Vertex> MyVertexs = new HashSet<Vertex>();// ����Ķ���
	private Map<Vertex, Set<Double>> LastMesseges = new HashMap<Vertex, Set<Double>>();// ������һ�ֽ��յ���Ϣ
	public Map<Vertex, Set<Double>> NowMesseges = new HashMap<Vertex, Set<Double>>();// ���浱ǰ�ֽ��յ���Ϣ
	public Map<Vertex,Set<Double>> Sendsessage = new HashMap<Vertex,Set<Double>>();
	// Set<Vertex>>();// ������Ӧ����
	private boolean WorkFlag = true;// ��Ծ
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
	 * ��ʼһ��
	 * 
	 * @return ������������true
	 */
	public boolean StartRun() {
		WorkSteps++;
		if (WorkSteps == 0) {// 0��
			Iterator<Vertex> allworks = this.MyVertexs.iterator();
			while (allworks.hasNext()) {
				Vertex myV = allworks.next();
				if (myV.GetSteps() == -1) {// 0��
					myV.SendMessageTo();
				} else {
					System.out.println("Works Wrong");
					return true;
				}
			}

		} else {// ��0��

			this.LastMesseges = this.NowMesseges;

			if (this.LastMesseges.isEmpty() || (!WorkFlag)) {// ��һ��δ���յ���Ϣ������Ϊ����Ծworker
				System.out.println(
						"��Worker����Ծ, Worker�������� (LastMesseges: " + this.LastMesseges.isEmpty() + "\t Worker��Ծ��־ �� " + WorkFlag+" )");
				return true;
			}

			// ѭ�����ֻ�Ծ���㣬ִ��compute
			Iterator<Entry<Vertex, Set<Double>>> ActiveWor = this.LastMesseges.entrySet().iterator();
			while (ActiveWor.hasNext()) {
				Map.Entry<Vertex, Set<Double>> entry = ActiveWor.next();
				entry.getKey().Compute(entry.getValue());// ִ��compute
				if(!Sendsessage.keySet().contains(entry.getKey())) {
					Set<Double> kkkk = new HashSet<>();
					kkkk.add(entry.getKey().GetVertexValue());
					Sendsessage.put(entry.getKey(), kkkk);//������Ϣ����
				}else {
					Sendsessage.get(entry.getKey()).add(entry.getKey().GetVertexValue());//������Ϣ����
				}
				entry.getKey().SendMessageTo();
			}
		}

		// ͳ�Ƶ�ǰ�����ж�����ܵ���Ϣ
		this.NowMesseges.clear();
		Iterator<Vertex> newWorks = this.MyVertexs.iterator();
		while (newWorks.hasNext()) {
			Vertex outV = newWorks.next();
			NowMesseges.put(outV, outV.GetInmessege());
			outV.ClearInmessege();// ���
		}
		this.NowMesseges = combine.combine(this.NowMesseges);//������ܶ��н���Combine

		// �жϱ��ֽ������Դ��ڻ�Ծ�Ķ���
		if (!this.NowMesseges.isEmpty()) {
			WorkFlag = true;// ��Ծ
		} else {
			WorkFlag = false;
		}

		return true;// ��ɹ���
	}

	/**
	 * �õ�Work���ֽ������Ƿ��Ծ
	 * 
	 * @return
	 */
	public boolean GetWordFlag() {
		return this.WorkFlag;
	}

}
