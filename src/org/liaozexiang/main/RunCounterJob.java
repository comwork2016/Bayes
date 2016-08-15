package org.liaozexiang.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.liaozexiang.filepath.UsedFilePath;
import org.liaozexiang.job.WordCounter;
import org.liaozexiang.util.VariableStore;
import org.liaozexiang.util.WriteSequenceFile;

public class RunCounterJob {

	public static void run(String trainPath) throws IOException, ClassNotFoundException, InterruptedException {
		// 将训练数据集写入到SequenceFile中
		WriteSequenceFile.writeByDirName(trainPath, UsedFilePath.counterSequenceFileOutputPath);
		Map<String, Integer> classFileCountMap = new HashMap<String, Integer>();// 记录文件数
		int trainFileCount = 0;//记录文件总数
		Configuration conf = new Configuration();
		Job counterJob = new Job(conf, "wordCounterJob");
		counterJob.setJarByClass(WordCounter.class);
		counterJob.setMapperClass(WordCounter.WordCountMapper.class);
		counterJob.setReducerClass(WordCounter.WordCountReducer.class);
		counterJob.setMapOutputKeyClass(Text.class);
		counterJob.setMapOutputValueClass(Text.class);
		counterJob.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(counterJob, new Path(UsedFilePath.counterSequenceFileOutputPath));
		// 遍历训练数据集，保存文件数等信息
		FileSystem trainFS = FileSystem.get(new Path(trainPath).toUri(), conf);
		FileStatus[] listDirFileStatus = trainFS.listStatus(new Path(trainPath));
		for (FileStatus dirStatus : listDirFileStatus) {
			FileStatus[] fileStatus = trainFS.listStatus(dirStatus.getPath());
			classFileCountMap.put(dirStatus.getPath().getName(), fileStatus.length);
			trainFileCount += fileStatus.length;
			MultipleOutputs.addNamedOutput(counterJob, dirStatus.getPath().getName(), TextOutputFormat.class, Text.class, IntWritable.class);
		}
		FileOutputFormat.setOutputPath(counterJob, new Path(UsedFilePath.counterOutputPath));
		boolean waitForCounterJobCompletion = counterJob.waitForCompletion(true);
		if (!waitForCounterJobCompletion) {
			System.out.println("counterJob error!");
			System.exit(1);
		}
		// 将文件数信息写入到文件中保存
		VariableStore.writeString("trainFileNum" + UsedFilePath.recordSeparator + trainFileCount, UsedFilePath.trainFileCountPath);
		VariableStore.writeMap(classFileCountMap, UsedFilePath.classFileCountMapPath);
	}
}
