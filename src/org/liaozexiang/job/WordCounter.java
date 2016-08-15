package org.liaozexiang.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.liaozexiang.filepath.UsedFilePath;
import org.liaozexiang.util.VariableStore;

public class WordCounter {

	public static class WordCountMapper extends Mapper<Text, Text, Text, Text> {

		private static Map<String, Integer> classWordCountMap;

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			classWordCountMap = new HashMap<String, Integer>();
		}

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split(UsedFilePath.recordSeparator);
			int wordCount = 0;
			for (String word : words) {
				// 对文本数据进行过滤
				if (!word.matches(".*[\\d+\\.+].*")) {
					// output: <单词,类名>
					wordCount++;
					context.write(new Text(word), key);
				}
			}
			classWordCountMap.put(key.toString(), wordCount);
		}

		@Override
		protected void cleanup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			VariableStore.writeMap(classWordCountMap, UsedFilePath.classWordCountMapPath);
		}
	}

	public static class WordCountReducer extends Reducer<Text, Text, Text, IntWritable> {

		private static int wordVariety;
		private static Map<String, Integer> classWordTimesMap;
		private MultipleOutputs<Text, IntWritable> mos;

		@Override
		protected void setup(Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			wordVariety = 0;
			classWordTimesMap = new HashMap<String, Integer>();
			mos = new MultipleOutputs<>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// key: 单词 value: 类名集合
			wordVariety++;
			Map<String, Integer> classTimesMap = new HashMap<String, Integer>();
			for (Text classification : values) {
				String w_c = key.toString() + UsedFilePath.stringSeparator + classification.toString();
				if (classTimesMap.get(w_c) == null) {
					classTimesMap.put(w_c, 1);
				} else {
					// 若单词在该分类中出现过，则直接加1
					classTimesMap.put(w_c, classTimesMap.get(w_c) + 1);
				}
			}
			for (Entry<String, Integer> entry : classTimesMap.entrySet()) {
				classWordTimesMap.put(entry.getKey(), entry.getValue());
				String[] w_c = entry.getKey().split(UsedFilePath.stringSeparator);
				mos.write(w_c[1], w_c[0], entry.getValue());
			}
		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// 保存任务中的变量信息
			VariableStore.writeString("wordVariety" + UsedFilePath.recordSeparator + wordVariety, UsedFilePath.wordVarietyPath);
			VariableStore.writeMap(classWordTimesMap, UsedFilePath.classWordTimesMapPath);
			mos.close();
		}
	}
}
