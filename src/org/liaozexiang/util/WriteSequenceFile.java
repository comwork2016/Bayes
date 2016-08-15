package org.liaozexiang.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.liaozexiang.filepath.UsedFilePath;
import org.apache.hadoop.io.Text;

public class WriteSequenceFile {
	/**
	 * 将文件夹名作为key，文件夹中的文件内容作为value写入SequenceFile中
	 */
	public static void writeByDirName(String strInputPath, String strOutputPath) throws IOException {
		Configuration conf = new Configuration();
		// 如果SequenceFile存在，则删除
		Path inputPath = new Path(strInputPath);
		Path outputPath = new Path(strOutputPath);
		FileSystem outputFS = outputPath.getFileSystem(conf);
		if (outputFS.exists(outputPath)) {
			outputFS.delete(outputPath, true);
		}
		Text key = new Text();
		Text val = new Text();
		Writer writer = null;
		try {
			writer = SequenceFile.createWriter(outputFS, conf, outputPath, key.getClass(), val.getClass());
			FileSystem inputFS = inputPath.getFileSystem(conf);
			FileStatus[] classDirStatuses = inputFS.listStatus(inputPath);
			for (FileStatus classDirStatus : classDirStatuses) {
				FileSystem fileFS = classDirStatus.getPath().getFileSystem(conf);
				FileStatus[] fileStatuses = fileFS.listStatus(classDirStatus.getPath());
				StringBuffer sb = new StringBuffer();
				for (FileStatus fileStatus : fileStatuses) {
					long fileLen = fileStatus.getLen();
					FSDataInputStream in = null;
					try {
						in = outputFS.open(fileStatus.getPath());
						byte[] contents = new byte[(int) fileLen];
						IOUtils.readFully(in, contents, 0, (int) fileLen);
						String fileContent = new String(contents, 0, (int) fileLen);
						// 去除文件内容中的换行符，并作为SequenceFile的Value
						sb.append(fileContent.replaceAll("\r\n", UsedFilePath.recordSeparator));
					} finally {
						IOUtils.closeStream(in);
					}
				}
				key.set(classDirStatus.getPath().getName());
				val.set(sb.toString());
				writer.append(key, val);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * 将中间文件夹名作为key，文件夹中的文件内容作为value写入SequenceFile中
	 */
	public static void writeInterFileByFileName(String strInputPath, String strOutputPath) throws IOException {
		Path inputPath = new Path(strInputPath);
		Path outputPath = new Path(strOutputPath);
		Configuration conf = new Configuration();
		// 如果SequenceFile存在，则删除
		FileSystem outputFS = outputPath.getFileSystem(conf);
		if (outputFS.exists(outputPath)) {
			outputFS.delete(outputPath, true);
		}
		Text key = new Text();
		Text val = new Text();
		Writer writer = null;
		try {
			writer = SequenceFile.createWriter(outputFS, conf, outputPath, key.getClass(), val.getClass());
			FileSystem inputFS = inputPath.getFileSystem(conf);
			FileStatus[] fileStatuses = inputFS.listStatus(inputPath);
			for (FileStatus fileStatus : fileStatuses) {
				StringBuffer sb = new StringBuffer();
				String fileName = fileStatus.getPath().getName();
				// 不处理MapReduce程序中产生的日志文件
				if (fileName.startsWith("_") || fileName.startsWith("part-")) {
					continue;
				}
				String className = fileName.substring(0, fileName.indexOf("-"));
				FSDataInputStream in = null;
				BufferedReader br = null;
				try {
					in = outputFS.open(fileStatus.getPath());
					br = new BufferedReader(new InputStreamReader(in));
					String line = "";
					while ((line = br.readLine()) != null) {
						sb.append(line.replaceAll("	", UsedFilePath.stringSeparator) + UsedFilePath.recordSeparator);
					}
				} finally {
					IOUtils.closeStream(in);
					IOUtils.closeStream(br);
				}
				key.set(className);
				val.set(sb.toString());
				writer.append(key, val);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * 将文件名作为key，文件中的文件内容作为value写入SequenceFile中
	 */
	public static void writeByFileName(String strInputPath, String strOutputPath) throws IOException {
		Configuration conf = new Configuration();
		// 如果SequenceFile存在，则删除
		Path inputPath = new Path(strInputPath);
		Path outputPath = new Path(strOutputPath);
		FileSystem outputFS = outputPath.getFileSystem(conf);
		if (outputFS.exists(outputPath)) {
			outputFS.delete(outputPath, true);
		}
		Text key = new Text();
		Text val = new Text();
		Writer writer = null;
		try {
			writer = SequenceFile.createWriter(outputFS, conf, outputPath, key.getClass(), val.getClass());
			FileSystem inputFS = inputPath.getFileSystem(conf);
			FileStatus[] classDirStatuses = inputFS.listStatus(inputPath);
			for (FileStatus classDirStatus : classDirStatuses) {
				FileSystem fileFS = classDirStatus.getPath().getFileSystem(conf);
				FileStatus[] fileStatuses = fileFS.listStatus(classDirStatus.getPath());
				for (FileStatus fileStatus : fileStatuses) {
					long fileLen = fileStatus.getLen();
					FSDataInputStream in = null;
					StringBuffer sb = new StringBuffer();
					try {
						in = outputFS.open(fileStatus.getPath());
						byte[] contents = new byte[(int) fileLen];
						IOUtils.readFully(in, contents, 0, (int) fileLen);
						String fileContent = new String(contents, 0, (int) fileLen);
						// 去除文件内容中的换行符，并作为SequenceFile的Value
						sb.append(fileContent.replaceAll("\r\n", UsedFilePath.recordSeparator));
					} finally {
						IOUtils.closeStream(in);
					}
					key.set(fileStatus.getPath().getName());
					val.set(sb.toString());
					writer.append(key, val);
				}
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}
