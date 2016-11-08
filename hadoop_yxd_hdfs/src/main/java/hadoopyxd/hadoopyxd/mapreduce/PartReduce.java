
package hadoopyxd.hadoopyxd.mapreduce;  

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * ClassName:PartReduce <br/> 
 * Function: TODO (分区reduce实现). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月8日 下午6:25:16 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class PartReduce extends Reducer<IntWritable, LongWritable, Text, Text> {

	Text outkey = new Text();
	@Override
	protected void reduce(IntWritable key, Iterable<LongWritable> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		
		
		if(0==key.get()){//偶数
			outkey.set(" 0 :");
		}else if (1 == key.get()){//奇数
			outkey.set(" 1 :");
		}
		
		context.write(outkey, new Text(sum+""));
	}
}
  