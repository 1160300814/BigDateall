package Pregel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class Vertex {
	private double VertexValue;// 顶点的值
	private Map<Vertex, Double> OutEdge = new HashMap<Vertex, Double>();// 出边
	private Set<Double> InMessageValue = new HashSet<>();// 当前轮轮接收消息列表
	// private String OutMessageValue = "";//发送的消息的值

	private int VID;// 顶点的id
	private long superstep = -1;// Superstep的轮数

	private boolean Activeflag = true;// 顶点活跃状态，初始化为活跃

	public Vertex(int num, double str) {
		this.VID = num;
		this.VertexValue = str;
		Activeflag = true;
	}

	/**
	 * 自定义Compute()函数
	 */
	public abstract void Compute(Set<Double> str);

	/**
	 * 获取顶点的值
	 * 
	 * @return
	 */
	public double GetVertexValue() {
		return this.VertexValue;
	}

	/**
	 * 修改顶点的值
	 * 
	 * @param newV
	 */
	public void ChangeVertexValue(double newV) {
		this.VertexValue = newV;
	}

	/**
	 * 顶点接受消息队列
	 * 
	 * @param outM
	 */
	public void InMessage(Set<Double> outM) {
		this.InMessageValue = outM;
		// this.Activeflag = true;
	}

	/**
	 * 设置发送的消息
	 * 
	 * @param str
	 */
	/*
	 * public void SetOutMessage(String str) { this.OutMessageValue = str; }
	 */

	/**
	 * 发送消息
	 */
	public void SendMessageTo() {
		if (this.Activeflag) {// 是活跃状态，则发送消息
			this.superstep = this.superstep + 1;
			Iterator<Vertex> it = this.OutEdge.keySet().iterator();
			while (it.hasNext()) {
				Vertex outV = it.next();
				outV.SetInMessage(this.VertexValue);
			}

			// this.OutMessageValue = "";
			this.VoteToHalt();// 发送消息之后进入不活跃状态
		}
		return;
	}

	/**
	 * 进入inactive状态
	 */
	public void VoteToHalt() {
		this.Activeflag = false;
	}

	/**
	 * 重写equals,判断标准点的ID
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
	 * 重写hashCode
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.VID;
		return result;
	}

	/**
	 * 获取超级轮数
	 * 
	 * @return
	 */
	public long GetSteps() {
		return this.superstep;
	}

	/**
	 * 得到顶点编号
	 * 
	 * @return
	 */
	public int GetID() {
		return this.VID;
	}

	/**
	 * 清空当前消息列表
	 */
	public void ClearInmessege() {
		this.InMessageValue.clear();
	}

	/**
	 * 获得当前消息列表
	 */
	public Set<Double> GetInmessege() {
		return this.InMessageValue;
	}

	/**
	 * 添加出边
	 * 
	 * @param outV 终止点
	 * @param outD 边权值
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
