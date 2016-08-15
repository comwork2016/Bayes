package org.liaozexiang.main;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;

public class MainJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// PickTrainTestFiles.pick(UsedFilePath.inputPath,
		// UsedFilePath.trainPath, UsedFilePath.testPath);
		// 检查程序的参数
		if (args.length != 3) {
			System.err.println("Usage: Bayes <trainPath> <testPath> <out>");
			System.exit(2);
		}
		String trainPath = args[0];
		String testPath = args[1];
		String outPath = args[2];
		Date counterStartTime;
		Date calcProbStartTime;
		Date LearnStartTime;
		Date LearnEndTime;
		counterStartTime = new Date();
		RunCounterJob.run(trainPath);// 计算词频的MapReduce程序
		calcProbStartTime = new Date();
		RunCalcProJob.run();// 计算单词概率的MapReduce程序
		LearnStartTime = new Date();
		RunLearnJob.run(testPath);// 对测试文件进行分类的MapReduce程序
		LearnEndTime = new Date();
		Long counterTime = calcProbStartTime.getTime() - counterStartTime.getTime();
		Long calcProbTime = LearnStartTime.getTime() - calcProbStartTime.getTime();
		Long testTime = LearnEndTime.getTime() - LearnStartTime.getTime();
		EvaluationUtil.evaluate(outPath, counterTime, calcProbTime, testTime);// 对分类结果进行评估
	}
}