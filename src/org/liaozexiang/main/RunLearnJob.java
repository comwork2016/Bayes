package org.liaozexiang.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.liaozexiang.filepath.UsedFilePath;
import org.liaozexiang.job.LearnJob;
import org.liaozexiang.util.VariableStore;
import org.liaozexiang.util.WriteSequenceFile;

public class RunLearnJob {
	public static void run(String testPath) throws IOException, ClassNotFoundException, InterruptedException {
		// 将测试文件写入到SequenceFile中
		WriteSequenceFile.writeByFileName(testPath, UsedFilePath.learnSequenceFileOutputPath);
		Map<String, String> fileTrueClassMap = new HashMap<String, String>();
		Map<String, Integer> testClassCountMap = new HashMap<String, Integer>();
		Configuration conf = new Configuration();
		Job learnJob = new Job(conf, "LearnJob");
		learnJob.setJarByClass(LearnJob.class);
		learnJob.setMapperClass(LearnJob.LearnMapper.class);
		learnJob.setReducerClass(LearnJob.LearnReducer.class);
		learnJob.setMapOutputKeyClass(Text.class);
		learnJob.setMapOutputValueClass(Text.class);
		learnJob.setInputFormatClass(SequenceFileInputFormat.class);
		Path learnInputPath = new Path(testPath);
		getFileTrueClassAndAddNamedOutPut(learnJob, learnInputPath, fileTrueClassMap, testClassCountMap, conf);
		// 保存任务中的变量信息
		VariableStore.writeMap(fileTrueClassMap, UsedFilePath.fileTrueClassMapPath);
		VariableStore.writeMap(testClassCountMap, UsedFilePath.testClassCountMapPath);
		VariableStore.writeString("testFileNum" + UsedFilePath.recordSeparator + fileTrueClassMap.size(), UsedFilePath.testFileNumPath);
		SequenceFileInputFormat.addInputPath(learnJob, new Path(UsedFilePath.learnSequenceFileOutputPath));
		FileOutputFormat.setOutputPath(learnJob, new Path(UsedFilePath.learnOutputPath));
		boolean learnJobCompletion = learnJob.waitForCompletion(true);
		if (!learnJobCompletion) {
			System.out.println("learn Job error");
			System.exit(1);
		}
	}
	/**
	 * 获取测试文件的真是类别，统计分类测试文件的个数，并指定Reduce输出的文件名称
	 */
	private static void getFileTrueClassAndAddNamedOutPut(Job job, Path inputPath, Map<String, String> fileTrueClassMap, Map<String, Integer> testClassCountMap, Configuration conf) throws IOException {
		FileSystem fs = null;
		fs = FileSystem.get(inputPath.toUri(), conf);
		FileStatus[] status = fs.listStatus(inputPath);
		for (FileStatus fileStatus : status) {
			if (fileStatus.isDir()) {
				// 记录测试文件的个数
				testClassCountMap.put(fileStatus.getPath().getName(), fs.listStatus(fileStatus.getPath()).length);
				getFileTrueClassAndAddNamedOutPut(job, fileStatus.getPath(), fileTrueClassMap, testClassCountMap, conf);
			} else {
				fileTrueClassMap.put(fileStatus.getPath().getName(), fileStatus.getPath().getParent().getName());
				MultipleOutputs.addNamedOutput(job, fileStatus.getPath().getName().substring(0, fileStatus.getPath().getName().indexOf(".")), TextOutputFormat.class, Text.class, Text.class);
			}
		}
	}
}
