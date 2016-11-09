
package hadoopyxd.hadoopyxd.mapreduce;  


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 
 * ClassName:PartRun <br/> 
 * Function: TODO (). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月8日 下午6:30:15 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class PartRun {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// TODO Auto-generated method stub
        //第一步获取configuration
		Configuration cf = new Configuration();
		Job job = new Job(cf, "partRun");
		//制定运行主
		job.setJarByClass(PartRun.class);
		job.setMapperClass(PartMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//制定分区类
		job.setPartitionerClass(MyPartitioner.class);
		
		//开始设置reduce
		job.setReducerClass(PartReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//指定reduce数目
		job.setNumReduceTasks(2);
		
		//添加参数
		args = new String[]{
                "/yxd/sort.txt",
                "yxd/test4"
		};
		//
		FileInputFormat.addInputPaths(job, args[0]);
		
		Path outputDir = new Path(args[1]);  
        FileSystem fs = FileSystem.get(cf);  
        if(fs.exists(outputDir)){
        	fs.delete(outputDir, true);
        }
		//输出
        FileOutputFormat.setOutputPath(job, outputDir);
        
        //提交
        System.exit(job.waitForCompletion(true)? 0: 1);  
		
	
	}

}
  