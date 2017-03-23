
package com.ibeifeng.transformer.mr.inputformat;  
/** 
 * ClassName:IdentityReducer <br/> 
 * Function: TODO (). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2017年3月23日 下午2:43:27 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class IdentityReducer<Text, BytesWritable> extends Reducer<Text, BytesWritable, Text, BytesWritable> {

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		for (BytesWritable value : values) {
			context.write(key, value);
		}
	}
}