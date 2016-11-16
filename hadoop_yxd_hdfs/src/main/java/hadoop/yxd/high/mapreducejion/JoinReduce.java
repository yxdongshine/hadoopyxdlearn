
package hadoop.yxd.high.mapreducejion;  

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * ClassName:JoinReduce <br/> 
 * Function: TODO (reducer 实现 ). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月7日 下午5:16:49 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class JoinReduce extends Reducer<LongWritable, JoinDataFormat, NullWritable, Text> {

	
	@Override
	protected void reduce(
			LongWritable key,
			Iterable<JoinDataFormat> values,
			Context context)
			throws IOException, InterruptedException {
		String userInfo = "";
		String lastInfo = null ;
		Text text  = new Text();
		List<String > orders = new ArrayList<String>();
		if(values!=null){
			for (JoinDataFormat joinDataFormat : values) {
				if(Util.MARK_CINFO==joinDataFormat.getJionDataKey()){
					//如果是用户信息
					userInfo = joinDataFormat.getJionData();
				}else if(Util.MARK_CORDER ==joinDataFormat.getJionDataKey()){
					//订单信息
					orders.add(joinDataFormat.getJionData());
				}
				
			}
		}
		
		//循环迭代
		for (String orderInfo : orders) {
			lastInfo = key.toString()+userInfo+orderInfo;
			text.set(lastInfo);
			context.write(null, text);
		}
	}
}
  