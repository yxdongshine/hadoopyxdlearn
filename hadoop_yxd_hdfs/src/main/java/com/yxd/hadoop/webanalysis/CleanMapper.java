package com.yxd.hadoop.webanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	public CleanMapper() {
		// TODO Auto-generated constructor stub
	}
	public static int OK_LENGTH = 36;
	private Text outMap = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			 Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
         String[] arr  = value.toString().split("\t");
         
         if(arr!=null&&arr.length>=OK_LENGTH){
        	 if(arr[1]!=null&&arr[1].trim().length()>0){//判断url存在
            	 
        		 StringBuffer sb  = new StringBuffer();
           		 sb.append(arr[2]+" "+arr[13]+" "+arr[17]);
        		 outMap.set(sb.toString());
        		 context.write(key, outMap);
        	 }else{
        		 context.getCounter(SystemUtil.PV_COUNTER,SystemUtil.URL_MISSING).increment(1L);
        	 }
         }else{
        	 context.getCounter(SystemUtil.PV_COUNTER,SystemUtil.DATA_MISS).increment(1L);
         }
       
	
         
	}
	
	
	public static void main(String[] args) {
		String str = "71508281814590031	http://s.yhd.com/?tc=0.0.12.2704_13852075_5.13&tp=1.1.16.0.5.KlBK557-10-6z7Ct	http://star.pclady.com.cn/135/1351735_3.html		3	B8SUYA48VDXS8KAE9Y1SVBC3HVBS3JRV9VZX		1.1.16.0.5.KlBK557-10-6z7Ct	0.0.12.2704_13852075_5.13		P8BZ4AU4EJBZY3BBUQ3GHBACPBYWRF8Q			39.180.50.118				2015-08-28 19:14:59		http://star.pclady.com.cn/135/1351735_3.html	1				1000					Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)	Win32							北京市	2		2015-08-28 19:14:59	北京市						58	HSl7CKz-00-EmKCv	0														1024*768											1									"
				+ "1		1199119844687";
		
		String[] arr  = str.toString().split("\t");
        
        if(arr!=null&&arr.length>=OK_LENGTH){
       	 if(arr[1]!=null&&arr[1].trim().length()>0){//判断url存在
           	 
       		 StringBuffer sb  = new StringBuffer();
       		 sb.append(arr[2]+" "+arr[13]+" "+arr[17]);
       		System.out.println(sb.toString());
       	  }
	     }
	}   
}
