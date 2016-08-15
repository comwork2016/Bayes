package org.liaozexiang.main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
import org.liaozexiang.job.CalcProb;
import org.liaozexiang.util.WriteSequenceFile;

public class RunCalcProJob {
	public static void run() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// 将词频信息写入到SequenceFile文件中
		WriteSequenceFile.writeInterFileByFileName(UsedFilePath.counterOutputPath, UsedFilePath.calcProbSequenceFileOutputPath);
		Configuration conf = new Configuration();
		Job calcProbJob = new Job(conf, "calcProJob");
		calcProbJob.setJarByClass(CalcProb.class);
		calcProbJob.setMapperClass(CalcProb.ProbMapper.class);
		calcProbJob.setReducerClass(CalcProb.ProbReducer.class);
		calcProbJob.setMapOutputKeyClass(Text.class);
		calcProbJob.setMapOutputValueClass(IntWritable.class);
		calcProbJob.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(calcProbJob, new Path(UsedFilePath.calcProbSequenceFileOutputPath));
		FileSystem counterfs = FileSystem.get(new URI(UsedFilePath.counterOutputPath), conf);
		FileStatus[] counterStatus = counterfs.listStatus(new Path(UsedFilePath.counterOutputPath));
		for (FileStatus fileStatus : counterStatus) {
			String fileName = fileStatus.getPath().getName();
			if (!fileName.startsWith("_") && !fileName.startsWith("part-")) {// 去掉hadoop生成的部分文件
				MultipleOutputs.addNamedOutput(calcProbJob, fileName.substring(0, fileName.indexOf("-")), TextOutputFormat.class, Text.class, IntWritable.class);
			}
		}
		Path bayesOutputPath = new Path(UsedFilePath.calcProbOutputPath);
		FileOutputFormat.setOutputPath(calcProbJob, bayesOutputPath);
		boolean completionBayes = calcProbJob.waitForCompletion(true);
		if (!completionBayes) {
			System.out.println("BayesJob error!");
			System.exit(1);
		}
	}
}
