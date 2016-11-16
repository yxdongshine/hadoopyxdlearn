
package hadoopyxd.hadoopyxd.mapreduce;  

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * ClassName:PartMapper <br/> 
 * Function: TODO (分区mapper). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月8日 下午6:19:38 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class PartMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

	int rowNumbe = 0;
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
        rowNumbe++;//行号增加1 
        
    	String[] valueArr = value.toString().split(",");
		if(valueArr.length>=3){
			long  cost = Long.parseLong(valueArr[2]);
			context.write(new IntWritable(rowNumbe), new LongWritable(cost));
		}
        
	}
}
  