
package com.ibeifeng.transformer.mr.inputformat;  
/** 
 * ClassName:WholeSmallfilesMapper <br/> 
 * Function: TODO (). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2017年3月23日 下午2:42:10 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WholeSmallfilesMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

	private Text file = new Text();

	@Override
	protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
		String fileName = context.getConfiguration().get("map.input.file");
		file.set(fileName);
		context.write(file, value);
	}
}