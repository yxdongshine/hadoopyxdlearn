package com.yxd.hadoop.webanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PvReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public PvReducer() {
		// TODO Auto-generated constructor stub
	}
	
	private IntWritable sum =new IntWritable();
	private int sumInt  =0 ;
	@SuppressWarnings("unchecked")
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Reducer.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
        
		for (IntWritable intWritable : arg1) {
			sumInt += intWritable.get();
		}
		sum.set(sumInt);
		arg2.write(SystemUtil.KEY,sum);
	}
}
