
package hadoop.yxd.high.mapreducejion;  

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 
 * ClassName:Run <br/> 
 * Function: TODO (运行测试). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月2日 下午3:42:09 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class Run {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
        //第一步获取configuration
		Configuration cf = new Configuration();
		Job job = new Job(cf, "run");
		//制定运行主
		job.setJarByClass(Run.class);
		job.setMapperClass(JoinMapper.class);
		job.setMapOutputKeyClass(Long.class);
		job.setMapOutputValueClass(JoinDataFormat.class);
		//开始设置reduce
		job.setReducerClass(JoinReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
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
  