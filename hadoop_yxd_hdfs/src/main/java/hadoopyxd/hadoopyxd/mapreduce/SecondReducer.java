
package hadoopyxd.hadoopyxd.mapreduce;  

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * ClassName:SecondReducer <br/> 
 * Function: TODO (). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月2日 下午3:34:56 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class SecondReducer extends Reducer<SecondSortClass, IntWritable, Text, Text>{

	@Override
	protected void reduce(SecondSortClass arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		 Text key = new Text();  
		 Text value = new Text(); 
        //将后面值组合 
		StringBuffer sb =new StringBuffer();
		for (IntWritable intWritable : arg1) {
			sb.append(","+intWritable);
			System.out.println(intWritable);
		}
		
		key.set(arg0.getFirst());
		value.set(sb.toString());
		//输出
		arg2.write(key,value);
	}
}
  