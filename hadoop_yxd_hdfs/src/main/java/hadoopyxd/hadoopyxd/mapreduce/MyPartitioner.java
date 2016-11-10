
package hadoopyxd.hadoopyxd.mapreduce;  

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/** 
 * ClassName:MyPartitioner <br/> 
 * Function: TODO (自定义分区). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月2日 下午4:09:37 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class MyPartitioner extends Partitioner<IntWritable,LongWritable>{

	/**
	 * 根据每一行的偏移量 奇偶数来分区
	 */
	@Override
	public int getPartition(IntWritable arg0,LongWritable arg1,  int arg2) {
		// TODO Auto-generated method stub
		
		if( arg0.get() % 2 == 0) {  
			arg0.set(0);  
            return 0;  
        } else {  
        	arg0.set(1);  
            return 1;  
        }  
		
	}

}
  