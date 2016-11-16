
package hadoop.yxd.high.mapreducejion;  

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * ClassName:JoinMapper <br/> 
 * Function: TODO (实现join mapper代码). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年11月7日 下午4:05:26 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class JoinMapper extends Mapper<LongWritable, Text, LongWritable, JoinDataFormat> {

	LongWritable outKey  = new LongWritable();
	
	JoinDataFormat jdf = null;
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		if(value.toString()!=null&&value.toString().trim().length()>0){
			String valueStr = value.toString().trim();
			String[] valueArr = valueStr.split(",");
			if(valueArr!=null&&(valueArr.length==3||valueArr.length==4)){
				//这里要根据字段数区别是订单信息4 还是用户信息3 
				outKey.set(Long.parseLong(valueArr[0]));//0 一定是用户编号
				if(valueArr.length==3){//标识用户信息
					String outValue = valueArr[1]+","+valueArr[2];
					jdf = new JoinDataFormat(Util.MARK_CINFO, outValue);
				}else if (valueArr.length==4){//订单信息
					String outOrderValue = valueArr[2]+","+valueArr[3];
					jdf = new JoinDataFormat(Util.MARK_CORDER, outOrderValue);
				}
				
				//输出
				context.write(outKey, jdf);
			}

		}else{//这里可以添加制定会议统计器
			//context.getCounter(arg0, arg1)
		}
        
		
	}
}
  