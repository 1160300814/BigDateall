package Pregel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class Vertex {
	private double VertexValue;// �����ֵ
	private Map<Vertex, Double> OutEdge = new HashMap<Vertex, Double>();// ����
	private Set<Double> InMessageValue = new HashSet<>();// ��ǰ���ֽ�����Ϣ�б�
	// private String OutMessageValue = "";//���͵���Ϣ��ֵ

	private int VID;// �����id
	private long superstep = -1;// Superstep������

	private boolean Activeflag = true;// �����Ծ״̬����ʼ��Ϊ��Ծ

	public Vertex(int num, double str) {
		this.VID = num;
		this.VertexValue = str;
		Activeflag = true;
	}

	/**
	 * �Զ���Compute()����
	 */
	public abstract void Compute(Set<Double> str);

	/**
	 * ��ȡ�����ֵ
	 * 
	 * @return
	 */
	public double GetVertexValue() {
		return this.VertexValue;
	}

	/**
	 * �޸Ķ����ֵ
	 * 
	 * @param newV
	 */
	public void ChangeVertexValue(double newV) {
		this.VertexValue = newV;
	}

	/**
	 * ���������Ϣ����
	 * 
	 * @param outM
	 */
	public void InMessage(Set<Double> outM) {
		this.InMessageValue = outM;
		// this.Activeflag = true;
	}

	/**
	 * ���÷��͵���Ϣ
	 * 
	 * @param str
	 */
	/*
	 * public void SetOutMessage(String str) { this.OutMessageValue = str; }
	 */

	/**
	 * ������Ϣ
	 */
	public void SendMessageTo() {
		if (this.Activeflag) {// �ǻ�Ծ״̬��������Ϣ
			this.superstep = this.superstep + 1;
			Iterator<Vertex> it = this.OutEdge.keySet().iterator();
			while (it.hasNext()) {
				Vertex outV = it.next();
				outV.SetInMessage(this.VertexValue);
			}

			// this.OutMessageValue = "";
			this.VoteToHalt();// ������Ϣ֮����벻��Ծ״̬
		}
		return;
	}

	/**
	 * ����inactive״̬
	 */
	public void VoteToHalt() {
		this.Activeflag = false;
	}

	/**
	 * ��дequals,�жϱ�׼���ID
	 */
	@Override
	public boolean equals(Object that) {
		if (!(that instanceof Vertex)) {
			return false;
		}
		Vertex thatObject = (Vertex) that;
		return thatObject.VID == this.VID;
	}

	/**
	 * ��дhashCode
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.VID;
		return result;
	}

	/**
	 * ��ȡ��������
	 * 
	 * @return
	 */
	public long GetSteps() {
		return this.superstep;
	}

	/**
	 * �õ�������
	 * 
	 * @return
	 */
	public int GetID() {
		return this.VID;
	}

	/**
	 * ��յ�ǰ��Ϣ�б�
	 */
	public void ClearInmessege() {
		this.InMessageValue.clear();
	}

	/**
	 * ��õ�ǰ��Ϣ�б�
	 */
	public Set<Double> GetInmessege() {
		return this.InMessageValue;
	}

	/**
	 * ��ӳ���
	 * 
	 * @param outV ��ֹ��
	 * @param outD ��Ȩֵ
	 */
	public void AddOutEdge(Vertex outV, Double outD) {
		this.OutEdge.put(outV, outD);
	}

	public void SetInMessage(double outM) {
		this.InMessageValue.add(outM);
		// this.Activeflag = true;
	}
	
	public Map<Vertex, Double> GetOutEdge(){
		return OutEdge;
	}

}
