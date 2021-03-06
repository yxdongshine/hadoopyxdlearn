package hive.yxd.high.udf;

import org.apache.hadoop.hive.ql.exec.UDF;


/***
 * 
 * 
 * ClassName: OneByOneUDF <br/> 
 * Function: TODO (用户自定函数  必须继承在udf ；  同时编写evaluate方法). <br/> 
 * Reason: TODO (). <br/> 
 * date: 2016年11月16日 下午4:00:46 <br/> 
 * 
 * @author yxd 
 * @version
 */
public class OneByOneUDF extends UDF {

	/**
	 * 
	 * evaluate:(). <br/> 
	 * TODO().<br/> 
	 * 
	 * @author yxd 
	 * @param inStr
	 * @return
	 */
	public String evaluate(String inStr){
		StringBuffer outStr = new StringBuffer("\"");
		if(inStr!=null&&inStr.trim().length()>0){
			outStr.append(inStr.trim()+"\"");
		}
		
		return outStr.toString();
	}
	
	
	public static void main(String[] args) {
		String inStr = "0.0.0.0";
		String outStr= new OneByOneUDF().evaluate(inStr);
		System.out.println(outStr);
	}
}
