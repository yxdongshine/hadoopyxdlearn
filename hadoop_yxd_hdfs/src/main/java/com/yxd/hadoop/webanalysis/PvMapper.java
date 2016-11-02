package com.yxd.hadoop.webanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PvMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	public PvMapper() {
		// TODO Auto-generated constructor stub
	}
	public static int OK_LENGTH = 16;
	private Text outKey = new Text();
	private IntWritable outMap = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
         String[] arr  = value.toString().split(" ");
         if(arr!=null&&arr.length>=OK_LENGTH){
        	 outKey.set(arr[1]);
         }
         context.write(outKey, outMap);
	
	}
}
