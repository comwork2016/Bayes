package org.liaozexiang.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class PickTrainTestFiles {

	public static void pick(String inputPath, String str_outputTrainPath, String str_outputTestPath) {
		Path outputTrainPath = new Path(str_outputTrainPath);
		Path outputTestPath = new Path(str_outputTestPath);
		Configuration conf = new Configuration();
		File inputDir = new File(inputPath);
		File[] listClassDirs = inputDir.listFiles();
		for (File classDir : listClassDirs) {
			File[] listClassFiles = classDir.listFiles();
			if (listClassFiles.length > 20) {
				List<Integer> shuffledIndex = getUniqueRandomSequence(listClassFiles.length);
				// first 50% files as train set
				try {
					int i = 0;
					for (; i < Math.ceil(shuffledIndex.size() / 2f); i++) {
						File file = listClassFiles[shuffledIndex.get(i)];
						FileSystem dstFS = FileSystem.get(outputTrainPath.toUri(), conf);
						Path dst = new Path(outputTrainPath.toString() + file.toString().substring(file.toString().indexOf(inputPath) + inputPath.length() + 1));
						FileUtil.copy(file, dstFS, dst, false, conf);
						System.out.println("moving to train set:" + dst.toString());
					}
					// another 50% files as test set
					for (; i < shuffledIndex.size() && i < Math.ceil(shuffledIndex.size() / 2f) + 20; i++) {
						File file = listClassFiles[shuffledIndex.get(i)];
						FileSystem dstFS = FileSystem.get(outputTestPath.toUri(), conf);
						Path dst = new Path(outputTestPath.toString() + file.toString().substring(file.toString().indexOf(inputPath) + inputPath.length() + 1));
						FileUtil.copy(file, dstFS, dst, false, conf);
						System.out.println("moving to test set:" + dst.toString());
					}
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("copy files to hdfs error");
					System.exit(1);
				}
			} else {
				// for (int i = 0; i < listClassFiles.length; i++) {
				// File file = listClassFiles[i];
				// try {
				// FileSystem dstFS = FileSystem.get(outputTrainPath.toUri(),
				// conf);
				// Path dst = new Path(outputTrainPath.toString() +
				// file.toString()
				// .substring(file.toString().indexOf(inputPath) +
				// inputPath.length() + 1));
				// FileUtil.copy(file, dstFS, dst, false, conf);
				// System.out.println("moving to train set:" + dst.toString());
				// } catch (IOException e) {
				// // TODO Auto-generated catch block
				// e.printStackTrace();
				// }
				// }
			}
		}
		System.out.println("moving complete");
	}

	/**
	 * generting unique sequence of Ingeter
	 * 
	 * @param bound
	 * @return
	 */
	private static List<Integer> getUniqueRandomSequence(int bound) {
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < bound; i++) {
			list.add(i);
		}
		Collections.shuffle(list);
		return list;
	}
}
