package org.liaozexiang.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.liaozexiang.filepath.UsedFilePath;
import org.liaozexiang.util.VariableStore;

public class CalcProb {
	public static class ProbMapper extends Mapper<Text, Text, Text, IntWritable> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// <类名,单词_词频>
			String fileName = key.toString();
			Text className = new Text(fileName);
			String[] w_ts = value.toString().split(UsedFilePath.recordSeparator);
			for (String w_t : w_ts) {
				String[] words = w_t.split(UsedFilePath.stringSeparator);
				// <单词_类别,词频>
				context.write(new Text(words[0] + UsedFilePath.stringSeparator + className), new IntWritable(Integer.parseInt(words[1])));
			}
		}
	}

	public static class ProbReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

		private static int wordVariety;
		private static Map<String, Integer> classWordCountMap;
		private static Map<String, Float> wordClassProbMap;
		private MultipleOutputs<Text, FloatWritable> mos;

		@Override
		protected void setup(Reducer<Text, IntWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
			//从文件中读取上一个任务中的变量信息
			wordVariety = VariableStore.readInteger(UsedFilePath.wordVarietyPath);
			classWordCountMap = VariableStore.readIntegerMap(UsedFilePath.classWordCountMapPath);
			wordClassProbMap = new HashMap<String, Float>();
			mos = new MultipleOutputs<>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
			// <单词_类别,词频>
			String word_classification = key.toString();
			String[] w_c = word_classification.split(UsedFilePath.stringSeparator);
			for (IntWritable times : values) {
				// 计算单词属于类别的概率，并取对数
				int iTimes = Integer.parseInt(times.toString());
				Integer classWordCount = classWordCountMap.get(w_c[1]);
				float prob = (iTimes + 1f) / (classWordCount + wordVariety + 0.0f);
				float logProb = (float) Math.log(prob);
				mos.write(w_c[1], new Text(w_c[0]), new FloatWritable(logProb));
				wordClassProbMap.put(key.toString(), logProb);
			}
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
			VariableStore.writeMap(wordClassProbMap, UsedFilePath.wordClassProbMapPath);
			mos.close();
		}
	}

}
