package org.liaozexiang.evaluation;

import java.util.ArrayList;
import java.util.List;

public class BayesEvaluation {
	private String classification;

	private List<String> tpList = new ArrayList<String>();// 被分到该类且分类正确的文件
	private List<String> fpList = new ArrayList<String>();// 被分到该类但分类错误的文件
	private List<String> fnList = new ArrayList<String>();// 未被分到该类，但是实际上属于该类的文件

	private int tp;
	private int fp;
	private int fn;

	private float p;
	private float r;
	private float f1;

	public BayesEvaluation(String classification) {
		super();
		this.classification = classification;
	}

	public String getClassification() {
		return classification;
	}

	public void setClassification(String classification) {
		this.classification = classification;
	}

	public List<String> getTpList() {
		return tpList;
	}

	public void setTpList(List<String> tpList) {
		this.tpList = tpList;
	}

	public List<String> getFpList() {
		return fpList;
	}

	public void setFpList(List<String> fpList) {
		this.fpList = fpList;
	}

	public List<String> getFnList() {
		return fnList;
	}

	public void setFnList(List<String> fnList) {
		this.fnList = fnList;
	}

	public float getP() {
		return p;
	}

	public float getR() {
		return r;
	}

	public float getF1() {
		return f1;
	}

	// 计算精确度P，召回率R和调和平均F1
	public void calc() {
		tp = tpList.size();
		fp = fpList.size();
		fn = fnList.size();
		p = (tp + 0.0f) / (tp + fp + 0.0f);
		r = (tp + 0.0f) / (tp + fn + 0.0f);
		f1 = 2 * p * r / (p + r);
	}

	@Override
	public String toString() {
		String tpListString = null;
		if (tpList.isEmpty()) {
			tpListString = "";
		} else {
			StringBuffer sbTpList = new StringBuffer();
			for (String string : tpList) {
				sbTpList.append(string + " ");
			}
			tpListString = sbTpList.substring(0, sbTpList.length() - 1);
		}
		String fpListString = null;
		if (fpList.isEmpty()) {
			fpListString = "";
		} else {
			StringBuffer sbfpList = new StringBuffer();
			for (String string : fpList) {
				sbfpList.append(string + " ");
			}
			fpListString = sbfpList.substring(0, sbfpList.length() - 1);
		}
		String fnListString = null;
		if (fnList.isEmpty()) {
			fnListString = "";
		} else {
			StringBuffer sbFnList = new StringBuffer();
			for (String string : fnList) {
				sbFnList.append(string + " ");
			}
			fnListString = sbFnList.substring(0, sbFnList.length() - 1);
		}
		return "classification=" + classification + ", p=" + p + ", r=" + r + ", f1=" + f1 + ", " + ", tpList="
				+ tpListString + ", fpList=" + fpListString + ", fnList=" + fnListString + ", tp=" + tp + ", fp=" + fp
				+ ", fn=" + fn;
	}
}
