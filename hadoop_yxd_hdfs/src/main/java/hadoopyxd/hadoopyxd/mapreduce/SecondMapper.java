
package hadoopyxd.hadoopyxd.mapreduce;  

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * ClassName:SecondMapper <br/> 
 * Function: TODO (). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月2日 下午3:28:58 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class SecondMapper extends Mapper<LongWritable, Text, SecondSortClass, IntWritable>{

	private IntWritable iw3 = new IntWritable();
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String[] valueArr = value.toString().split(",");
		if(valueArr.length>=3){
			iw3.set(Integer.parseInt(valueArr[2]));
			SecondSortClass ssc =new SecondSortClass(valueArr[1], Integer.parseInt(valueArr[2]));
			context.write(ssc, iw3);
		}
	}
}
  