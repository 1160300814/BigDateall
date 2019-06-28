package Pregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Master {
	private List<Worker> MyWorkers = new ArrayList<Worker>();// Worker�б�
	private int WorksNum = 1;// �ּ���worksWorks
	private List<Set<Vertex>> Partall = new ArrayList<>();// ���������
	private Set<Vertex> graphall;// ȫͼ==ȫ��
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
		if (Partall.size() != ActiveWorksNum) {// ����
			System.out.println("Master��Wrong��Partall.size() != ActiveWorksNum ! ");
			return;
		}

		for (int i = 0; i < ActiveWorksNum; i++) {// ��ʼ��worker
			Worker work = new Worker(i, Partall.get(i), CC);
			System.out.println("Work: " + i + "\t Vertex: " + Partall.get(i).size() + "\t");// 3.10�yӋ��
			long Esum = 0;
			for (Vertex EK : Partall.get(i)) {
				Esum += EK.GetOutEdge().keySet().size();
			}
			System.out.println("Edge: " + Esum);// 3.10�yӋ��

			this.MyWorkers.add(work);
		}

		// boolean state = true;
		int getworkerback = 0;
		long SendM = 0;
		long GetM = 0;
		boolean isnextactiveworker = true;
		while (isnextactiveworker) {// ��ʼһ��BSP
			Maststeps++;
			isnextactiveworker = false;
			for (int i = 0; i < ActiveWorksNum; i++) {
				Worker work = this.MyWorkers.get(i);
				long Wstart = System.currentTimeMillis();// һ��WORKER��ʼʱ��
				boolean workrea = work.StartRun();
				long Wend = System.currentTimeMillis();// һ��WORKER����ʱ��
				System.out.println("BSP: " + isnextactiveworker + "\t Worker�� " + i + "\t Total Time:");// 3.10�yӋ��
				System.out.println(Wend - Wstart);// 3.10�yӋ��
				// ͳ��works�����
				if (workrea) {// n
					getworkerback++;
					for (Set<Double> SN : work.Sendsessage.values()) {
						SendM += SN.size();
					}
					for (Set<Double> GN : work.NowMesseges.values()) {
						GetM += GN.size();
					}
					System.out.println("Send: " + SendM + "\t Get�� " + GetM + "\t Total Message: " + (GetM + SendM));// 3.10�yӋ��

				}
				// �ж���һ�����л�Ծworker��BSPδͣ
				if (work.GetWordFlag()) {
					isnextactiveworker = true;
				}
			}

			if (getworkerback != this.WorksNum) {// �ж��Ƿ���workerδ���
				System.out.println("BSP: " + isnextactiveworker + "\t δ���Worker�� " + (this.WorksNum - getworkerback));
			}

		}

	}

	/**
	 * ��ȫ���� �������
	 * 
	 * @param graph @return[���㣩���㣩][���㣩���㣩][......]
	 */
	public List<Set<Vertex>> Partition() {
		List<Set<Vertex>> part = new ArrayList<>();
		Set<Vertex> gk = graphall;// ͼ����ʼ��
		Set<Vertex> tmp = new HashSet<>();
		Map<Integer, Set<Vertex>> hold = new HashMap<>();// ��ʼ�����������
		// ����������䵽��ͬ�ķ��飨���ֵ���㼯��
		for (Vertex key : gk) {
			int rd = (int) Math.random() * WorksNum;// �����
			if (!hold.containsKey(rd))
				hold.put(rd, new HashSet<Vertex>());
			hold.get(rd).add(key);
		}
		// ��ͬһ��ĵ�����[���㣩���㣩][���㣩���㣩][......
		for (Integer key : hold.keySet()) {// ͬһ��
			tmp = new HashSet<>();
			for (Vertex item : hold.get(key)) {// ����ͬһ���еĵ�
				tmp.add(item);// ͬһ��ġ����㣩���㣩���㣩......��
			}
			part.add(tmp);
		}
		return part;
	}

}
