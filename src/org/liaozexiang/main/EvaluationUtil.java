package org.liaozexiang.main;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.liaozexiang.evaluation.BayesEvaluation;
import org.liaozexiang.filepath.UsedFilePath;
import org.liaozexiang.util.VariableStore;

public class EvaluationUtil {

	public static void evaluate(String outPath, Long counterTime, Long calcProbTime, Long testTime) {
		// 对分类器的结果进行评估
		Map<String, BayesEvaluation> evaluationMap = new HashMap<String, BayesEvaluation>();
		Map<String, String> fileTrueAndTestClassMap = VariableStore
				.readStringMap(UsedFilePath.fileTrueAndTestClassMapPath);
		for (Entry<String, String> entry : fileTrueAndTestClassMap.entrySet()) {
			String[] trueTestClasses = entry.getValue().split(UsedFilePath.stringSeparator);
			String trueClass = trueTestClasses[0];
			String testClass = trueTestClasses[1];
			BayesEvaluation testClassEvaluation = evaluationMap.get(testClass);
			if (testClassEvaluation == null) {
				testClassEvaluation = new BayesEvaluation(testClass);
				evaluationMap.put(testClass, testClassEvaluation);
			}
			if (trueClass.equals(testClass)) {
				//分类正确时，加入到类名的TP列表中
				testClassEvaluation.getTpList().add(entry.getKey());
			} else {
				//分类不正确时，加入到错误分类的FP列表中，并在正确分类的FN列表中添加该文件信息
				testClassEvaluation.getFpList().add(entry.getKey());
				BayesEvaluation trueClassEvaluation = evaluationMap.get(trueClass);
				if (trueClassEvaluation == null) {
					trueClassEvaluation = new BayesEvaluation(trueClass);
					evaluationMap.put(trueClass, trueClassEvaluation);
				}
				trueClassEvaluation.getFnList().add(entry.getKey());
			}
		}
		//保存程序运行时间信息
		StringBuffer sb = new StringBuffer();
		sb.append("Counter Time:" + counterTime / 1000f / 60f + "min" + "\n");
		sb.append("calcProb Time:" + calcProbTime / 1000f / 60f + "min" + "\n");
		sb.append("test Time:" + testTime / 1000f / 60f + "min" + "\n");
		//计算精确度P，召回率R和调和平均F1
		for (Entry<String, BayesEvaluation> entry : evaluationMap.entrySet()) {
			BayesEvaluation evaluation = entry.getValue();
			evaluation.calc();
			sb.append(evaluation.getClassification() + "	P:" + evaluation.getP() + "	R:" + evaluation.getR() + "	F1:"
					+ evaluation.getF1() + "\n");
		}
		VariableStore.writeString(sb.toString(), outPath);
		VariableStore.writeEvaluationMap(evaluationMap, UsedFilePath.evaluationMapPath);
	}
}
