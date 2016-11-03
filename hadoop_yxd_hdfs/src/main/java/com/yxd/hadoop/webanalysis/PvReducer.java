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
	private Text outKey = new Text(SystemUtil.KEY);
	private int sumInt  =0 ;
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
        
		for (IntWritable intWritable : arg1) {
			sumInt += intWritable.get();
		}
		sum.set(sumInt);
		arg2.write(outKey,sum);
	}
}
