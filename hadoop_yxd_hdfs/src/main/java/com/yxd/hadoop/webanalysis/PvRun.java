package com.yxd.hadoop.webanalysis;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.yxd.hadoop.webanalysis.SystemUtil;
import com.yxd.hadoop.webanalysis.PvReducer;
import com.yxd.hadoop.webanalysis.PvMapper;
public class PvRun {
	
	public int run (String[] args) throws Exception{
		int re = 0;
		Configuration conf = SystemUtil.getConf();
		
		Job job = new Job(conf, PvRun.class.getSimpleName());
		job.setJarByClass(PvRun.class);
		
		job.setMapperClass(PvMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(PvReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path input = new Path(args[0]);
        FileInputFormat.addInputPath(job, input);
        
        Path outPath  = new Path(args[1]);
        FileSystem fs = SystemUtil.getFs();
        if(fs.exists(outPath)){
        	fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        
        re = job.waitForCompletion(true)?0:1;
		return re ;
	}

	
	public static void main(String[] args) throws Exception {

		// 传递两个参数，设置路径
		args = new String[] {
				"hdfs://hadoop1:9000/yxd/yarn",
				"hdfs://hadoop1:9000/yxd/test6" };

		// run job
		int status = new PvRun().run(args);

		System.exit(status);
	}
}
