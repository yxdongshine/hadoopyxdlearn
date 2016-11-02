package com.yxd.hadoop.webanalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class SystemUtil {
	
	/**
	 * get conf
	 * @return
	 */
	public static Configuration getConf(){
		
		Configuration  conf = null;
		conf =new Configuration();
		
		return conf;
		
	}
	
	
	/**
	 * 
	 * @return
	 */
	public static FileSystem getFs(){
		FileSystem fs =null ;
		try {
			fs = FileSystem.get(getConf());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}

}
