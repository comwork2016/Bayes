package org.liaozexiang.filepath;

public class UsedFilePath {
	public static final String stringSeparator = "___";
	public static final String recordSeparator = "	";
	public static final String inputPath = "E:/courseware/hust/Hadoop/NBCorpus/Country";
	private static String hadoopBaseDir = "hdfs://115.156.158.8:9000/user/hadoop/";

	public static final String trainPath = hadoopBaseDir + "/Country/Train";
	public static final String testPath = hadoopBaseDir + "/Country/Test";
	// public static final String trainPath = haddoopBaseDir + "/TC/Train";
	// public static final String testPath = haddoopBaseDir + "/TC/Test";

	public static final String counterSequenceFileOutputPath = hadoopBaseDir + "/sequencefile/countersequncefile.seq";
	public static final String calcProbSequenceFileOutputPath = hadoopBaseDir
			+ "/sequencefile/calcprobsequncefile.seq";
	public static final String learnSequenceFileOutputPath = hadoopBaseDir + "/sequencefile/learnsequncefile.seq";

	public static final String counterOutputPath = hadoopBaseDir + "/output/counteroutput";
	public static final String calcProbOutputPath = hadoopBaseDir + "/output/calcproboutput";
	public static final String learnOutputPath = hadoopBaseDir + "/output/learnoutput";

	// counter Result files
	public static final String wordVarietyPath = hadoopBaseDir + "/interResult/counterResult/wordVariety.txt";
	public static final String trainFileCountPath = hadoopBaseDir + "/interResult/counterResult/trainFileCount.txt";
	public static final String classFileCountMapPath = hadoopBaseDir
			+ "/interResult/counterResult/classFileCountMap.txt";
	public static final String classWordCountMapPath = hadoopBaseDir
			+ "/interResult/counterResult/classWordCountMap.txt";
	public static final String classWordTimesMapPath = hadoopBaseDir
			+ "/interResult/counterResult/classWordTimesMap.txt";

	// calcProb Result files
	public static final String wordClassProbMapPath = hadoopBaseDir
			+ "/interResult/calcProbResult/wordClassProbMap.txt";

	// testFile Result files
	public static final String testFileNumPath = hadoopBaseDir + "/interResult/testFileResult/testFileNum.txt";
	public static final String docClassProbMapPath = hadoopBaseDir + "/interResult/testFileResult/docClassProbMap.txt";
	public static final String fileTrueClassMapPath = hadoopBaseDir
			+ "/interResult/testFileResult/fileTrueClassMap.txt";
	public static final String testClassCountMapPath = hadoopBaseDir
			+ "/interResult/testFileResult/testClassCountMap.txt";
	public static final String fileTrueAndTestClassMapPath = hadoopBaseDir
			+ "/interResult/testFileResult/fileTrueAndTestClassMap.txt";

	// evaluatioin Result files
	public static final String evaluationMapPath = hadoopBaseDir + "/interResult/evaluatioinResult/evaluationMap.txt";
}
