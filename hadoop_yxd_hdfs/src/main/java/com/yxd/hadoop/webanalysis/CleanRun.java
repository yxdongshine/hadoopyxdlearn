package com.yxd.hadoop.webanalysis;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;

import com.yxd.hadoop.webanalysis.PvReducer;
import com.yxd.hadoop.webanalysis.PvMapper;
public class CleanRun {
	
	@SuppressWarnings("deprecation")
	public int run (String[] args) throws Exception{
		int re = 0;
		Configuration conf = SystemUtil.getConf();
		
		Job job = new Job(conf, CleanRun.class.getSimpleName());
		job.setJarByClass(CleanRun.class);
		
		job.setMapperClass(CleanMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
/*		job.setReducerClass(PvReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);*/
		
		Path input = new Path(args[0]);
        FileInputFormat.addInputPath(job, input);
        
        Path outPath  = new Path(args[1]);
        FileSystem fs = SystemUtil.getFs();
        if(fs.exists(outPath)){
        	fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        
        re = job.waitForCompletion(true)?0:1;

		//get counter 
		Counters counters =job.getCounters();
		
		long dataCounter = counters.getGroup(SystemUtil.PV_COUNTER).findCounter(SystemUtil.DATA_MISS).getValue();
		long urlCounter = counters.getGroup(SystemUtil.PV_COUNTER).findCounter(SystemUtil.URL_MISSING).getValue();

		System.out.println("&&&&&datacounter&&&&"+dataCounter);
		System.out.println("&&&&&urlCounter&&&&"+urlCounter);
		return re ;
	}

	
	public static void main(String[] args) throws Exception {

		// 传递两个参数，设置路径
		args = new String[] {
				"hdfs://hadoop1:9000/projectdata/",
				"hdfs://hadoop1:9000/projectresult/" };

		// run job
		int status = new CleanRun().run(args);
		System.exit(status);
	}
}
