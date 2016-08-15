package org.liaozexiang.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.liaozexiang.filepath.UsedFilePath;
import org.liaozexiang.util.VariableStore;

public class LearnJob {
	public static class LearnMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class LearnReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		private static int trainFileCount;// 训练文件总数
		private static int wordVariety;// 单词种类数
		private static Map<String, Integer> classFileCountMap;// 分类中的文件数
		private static Map<String, Float> wordClassProbMap;// 单词概率
		private static Map<String, Integer> classWordsCountMap;// 分类中单词总数
		private static Map<String, String> fileTrueClassMap;// 文档的真是类别
		private static Map<String, Float> docClassProbMap;// 文档属于分类的概率
		private static Map<String, String> fileTrueAndTestClassMap;// 文档的真实类别和分类的类别

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// 从以前的任务中读取变量信息
			trainFileCount = VariableStore.readInteger(UsedFilePath.trainFileCountPath);
			wordVariety = VariableStore.readInteger(UsedFilePath.wordVarietyPath);
			classFileCountMap = VariableStore.readIntegerMap(UsedFilePath.classFileCountMapPath);
			wordClassProbMap = VariableStore.readFloatMap(UsedFilePath.wordClassProbMapPath);
			classWordsCountMap = VariableStore.readIntegerMap(UsedFilePath.classWordCountMapPath);
			fileTrueClassMap = VariableStore.readStringMap(UsedFilePath.fileTrueClassMapPath);
			docClassProbMap = new HashMap<String, Float>();
			fileTrueAndTestClassMap = new HashMap<String, String>();
			mos = new MultipleOutputs<>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// <fileName,words>
			Map<String, Float> oneDocClassProbMap = new HashMap<String, Float>();
			for (Text text : values) {
				String[] words = text.toString().split(UsedFilePath.recordSeparator);
				for (String word : words) {
					if (!word.matches(".*[\\d+\\.+].*")) {// 过滤文档中的单词
						for (String className : classFileCountMap.keySet()) {
							String d_c = key.toString() + UsedFilePath.stringSeparator + className;
							if (oneDocClassProbMap.get(d_c) == null) {
								// 计算先验概率
								float priorProb = (float) Math.log(classFileCountMap.get(className) / (trainFileCount + 0.0f));
								oneDocClassProbMap.put(d_c, priorProb);
							}
							Float wordProb = 0f;
							String w_c = word.toString() + UsedFilePath.stringSeparator + className;
							if (wordClassProbMap.get(w_c) == null) {
								// 当文档中的单词未在训练集中出现时，用默认概率计算。
								wordProb = (float) Math.log(1f / (classWordsCountMap.get(className) + wordVariety + 0.0f));
							} else {
								wordProb = wordClassProbMap.get(w_c);
							}
							// 用对数相加计算文档属于某个分类的概率
							oneDocClassProbMap.put(d_c, oneDocClassProbMap.get(d_c) + wordProb);
						}
					}
				}
			}
			// 挑选最大概率的分类
			float maxProb = 0f;
			String classification = "";
			for (String str : oneDocClassProbMap.keySet()) {
				maxProb = oneDocClassProbMap.get(str);
				classification = str.split(UsedFilePath.stringSeparator)[1];
				break;
			}
			for (Entry<String, Float> entry : oneDocClassProbMap.entrySet()) {
				if (entry.getValue() > maxProb) {
					maxProb = entry.getValue();
					classification = entry.getKey().split(UsedFilePath.stringSeparator)[1];
				}
				mos.write(key.toString().substring(0, key.toString().indexOf(".")), new Text(entry.getKey().split(UsedFilePath.stringSeparator)[1]), new Text(entry.getValue() + ""));
				docClassProbMap.put(entry.getKey(), entry.getValue());
			}
			fileTrueAndTestClassMap.put(key.toString(), fileTrueClassMap.get(key.toString()) + UsedFilePath.stringSeparator + classification);
		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			VariableStore.writeMap(docClassProbMap, UsedFilePath.docClassProbMapPath);
			VariableStore.writeMap(fileTrueAndTestClassMap, UsedFilePath.fileTrueAndTestClassMapPath);
			mos.close();
		}
	}
}
