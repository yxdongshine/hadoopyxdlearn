package com.yxd.hadoopproject.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ibeifeng.transformer.common.EventLogConstants;
import com.ibeifeng.transformer.common.GlobalConstants;
import com.ibeifeng.transformer.mr.etl.AnalysisDataMapper;
import com.ibeifeng.transformer.mr.etl.AnalysisDataRunner;
import com.ibeifeng.transformer.util.TimeUtil;

public class Run_project implements Tool{

	private Configuration conf = null;

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		//define my conf: hbase conf
		this.conf = HBaseConfiguration.create(conf);

	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// get conf
		Configuration conf = this.getConf();

		// process para
		processArgs(conf, args);
		Job job = Job.getInstance(conf, "hadoopproject");
		job.setJarByClass(Run_project.class);

		//设置mapper
		job.setMapperClass(RunToHbasemapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		// 设置reducer参数
		job.setNumReduceTasks(0); // 没有reducer设置为0


		// 设置输入信息
		this.setJobInputPaths(job);
		
		// 设置输出到hbase的参数信息
		this.setHBaseOutputConfig(job);
		
		
		// 成功返回0，失败返回-1, true的意思：阻塞等待job执行完成
		return job.waitForCompletion(true) ? 0 : -1;

	}
	/**
	 * 设置输出到hbase参数<br/>
	 * hbase按天分表：表名格式为： event_logs_20151220<br/>
	 * 由于是按天分表，所以需要使用java代码进行创建表<br/>
	 * 
	 * @param job
	 * @throws IOException
	 */
	private void setHBaseOutputConfig(Job job) throws IOException {
		Configuration conf = job.getConfiguration();
		// 获取要etl数据的日期市那一天
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
		// 格式化hbase的后缀
		String tableNameSuffix = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date),
				TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
		// 构建表名称
		String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;

		// 指定输出, 当不存在reducer的时候，reducer对于参数设置为null
		// 当本地提交集群运行的时候以及集群运行的时候，需要按照下面这行代码设置， addDependencyJars参数要求为true
//		TableMapReduceUtil.initTableReducerJob(tableName, null, job);

		// windows本地运行，需要使用下面代码, addDependencyJars参数要求为false
		 TableMapReduceUtil.initTableReducerJob(tableName, null, job, null,
		 null, null, null, false);

		// 更新hbase中对应表结构
		// 存在就删除，然后新建
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (Exception e) {
			throw new RuntimeException("创建hbaseadmin对象失败", e);
		}

		try {
			TableName tn = TableName.valueOf(tableName);
			HTableDescriptor htd = new HTableDescriptor(tn);
			// 设置family
			htd.addFamily(new HColumnDescriptor(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME));
			// 设置其他参数
			// 。。。。

			// 判断表是否存在
			if (admin.tableExists(tn)) {
				// 存在删除
				if (admin.isTableEnabled(tn)) {
					// 处于enabled状态
					admin.disableTable(tn); // 修改状态
				}
				// 删除表
				admin.deleteTable(tn);
			}

			// 创建表
			// 预分区
			// byte[][] keySplits = new byte[3][]; // 3个区
			// keySplits[0] = Bytes.toBytes("1"); // （负无穷大，1]
			// keySplits[1] = Bytes.toBytes("2"); // (1,2)
			// keySplits[2] = Bytes.toBytes("3"); // [2,+x)
			// admin.createTable(htd, keySplits);
			admin.createTable(htd);
		} catch (Exception e) {
			throw new RuntimeException("创建habse表失败", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (Exception e) {
					// 防止finaly语句块中的异常影响代码
				}
			}
		}
	}
	/**
	 * 设置job的输入参数<br/>
	 * 使用默认的InputFormat读取HDFS上的原始日志文件
	 * 
	 * @param job
	 * @throws IOException
	 */
	private void setJobInputPaths(Job job) throws IOException {
		Configuration conf = job.getConfiguration();
		// 获取要etl数据的日期是那一天
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
		// 文件名格式化
		String hdfsPath = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "yyyy/MM/dd");
		if (GlobalConstants.HDFS_LOGS_PATH_PREFIX.endsWith("/")) {
			// 以反斜杠结尾
			hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + hdfsPath;
		} else {
			// 没有反斜杠
			hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + "/" + hdfsPath;
		}
		// hdfs的格式为: /eventLogs/yyyy/MM/dd ==>
		// ${fs.defaultFS}/eventLogs/2015/12/20 ==>
		// hdfs://hadoop-senior01:8020/eventLogs/2015/12/20

		// 默认情况下，FileSystem是单例的(针对同一个schema: hdfs\file....),FileSystem在多个线程中是共享的
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(hdfsPath);
		if (fs.exists(inputPath)) {
			// 文件夹存在
			FileInputFormat.addInputPath(job, inputPath);
		} else {
			throw new RuntimeException("hdfs对应文件夹不存在:" + hdfsPath);
		}
		// 默认情况下，filesystem不要进行关闭操作，就是不要调用close方法 =====>>>>>> fs.close()不要调用
	}
	private void processArgs(Configuration conf, String[] args) {
		String date = null;
		for (int i = 0; i < args.length; i++) {
			if ("-d".equals(args[i])) {
				if (i + 1 < args.length) {
					date = args[++i];
					break;
				}
			}
		}

		if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
			date = TimeUtil.getYesterday(); 
		}
		//可以在conf里面自定义配置一些map key-value 参数
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}

	
	
	/**
	 * main方法，执行入口
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		test1(args); // 本地运行和集群运行
	}

	/**
	 * 正常运行
	 * 
	 * @param args
	 */
	public static void test1(String[] args) {
		try {
			// 调用执行
			int exitCode = ToolRunner.run(new Run_project(), args);
			if (exitCode == 0) {
				System.out.println("运行成功");
			} else {
				System.out.println("运行失败");
			}
		} catch (Exception e) {
			System.err.println("job运行异常" + e);
		}
	}
}
