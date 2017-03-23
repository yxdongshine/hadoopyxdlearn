
package com.ibeifeng.transformer.mr.inputformat;  
/** 
 * ClassName:WholeFileInputFormat <br/> 
 * Function: TODO (). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2017年3月23日 下午2:41:12 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		RecordReader<NullWritable, BytesWritable> recordReader = new WholeFileRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}
}