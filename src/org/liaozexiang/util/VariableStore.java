package org.liaozexiang.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.liaozexiang.evaluation.BayesEvaluation;
import org.liaozexiang.filepath.UsedFilePath;

public class VariableStore {

	//将中间的map变量保存到文件中
	public static <KEY, VALUE> void writeMap(Map<KEY, VALUE> map, String strPath) {
		Configuration conf = new Configuration();
		Path path = new Path(strPath);
		FSDataOutputStream os = null;
		try {
			FileSystem fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			os = fs.create(path);
			for (Entry<KEY, VALUE> entry : map.entrySet()) {
				String record = entry.getKey().toString() + UsedFilePath.recordSeparator + entry.getValue().toString();
				os.write(record.getBytes());
				os.write("\r\n".getBytes());
			}
			os.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	//将中间的文件数等变量以字符串的形式保存到文件中
	public static void writeString(String str, String strPath) {
		Configuration conf = new Configuration();
		Path path = new Path(strPath);
		FSDataOutputStream os = null;
		try {
			FileSystem fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			os = fs.create(path);
			os.write(str.getBytes());
			os.write("\r\n".getBytes());
			os.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	//保存评价结果
	public static void writeEvaluationMap(Map<String, BayesEvaluation> map, String strPath) {
		Configuration conf = new Configuration();
		Path path = new Path(strPath);
		FSDataOutputStream os = null;
		try {
			FileSystem fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			os = fs.create(path);
			for (Entry<String, BayesEvaluation> entry : map.entrySet()) {
				BayesEvaluation evaluation = entry.getValue();
				os.write(evaluation.toString().getBytes());
				os.write("\r\n".getBytes());
			}
			os.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	//将文件中的Map<String,String>类型记录读出
	public static Map<String, String> readStringMap(String strPath) {
		Configuration conf = new Configuration();
		Map<String, String> map = new HashMap<String, String>();
		Path path = new Path(strPath);
		FileSystem fs;
		FSDataInputStream in = null;
		BufferedReader br = null;
		try {
			fs = path.getFileSystem(conf);
			in = fs.open(path);
			br = new BufferedReader(new InputStreamReader(in));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] records = line.split(UsedFilePath.recordSeparator);
				map.put(records[0], records[1]);
			}
			return map;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	//将文件中的Map<String,Integer>类型记录读出
	public static Map<String, Integer> readIntegerMap(String strPath) {
		Configuration conf = new Configuration();
		Map<String, Integer> map = new HashMap<String, Integer>();
		Path path = new Path(strPath);
		FileSystem fs;
		FSDataInputStream in = null;
		BufferedReader br = null;
		try {
			fs = path.getFileSystem(conf);
			in = fs.open(path);
			br = new BufferedReader(new InputStreamReader(in));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] records = line.split(UsedFilePath.recordSeparator);
				map.put(records[0], Integer.parseInt(records[1]));
			}
			return map;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	//将文件中的Map<String,Float>类型记录读出
	public static Map<String, Float> readFloatMap(String strPath) {
		Configuration conf = new Configuration();
		Map<String, Float> map = new HashMap<String, Float>();
		Path path = new Path(strPath);
		FileSystem fs;
		FSDataInputStream in = null;
		BufferedReader br = null;
		try {
			fs = path.getFileSystem(conf);
			in = fs.open(path);
			br = new BufferedReader(new InputStreamReader(in));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] records = line.split(UsedFilePath.recordSeparator);
				map.put(records[0], Float.parseFloat(records[1]));
			}
			return map;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	//将文件中的Integer类型记录读出
	public static Integer readInteger(String strPath) {
		Configuration conf = new Configuration();
		Path path = new Path(strPath);
		FileSystem fs;
		FSDataInputStream in = null;
		BufferedReader br = null;
		try {
			fs = path.getFileSystem(conf);
			in = fs.open(path);
			br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			return Integer.parseInt(line.split(UsedFilePath.recordSeparator)[1]);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}
