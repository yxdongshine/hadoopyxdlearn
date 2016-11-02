package hadoop_yxd.hadoop_yxd_hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class YxdHdfs {

	public static FileSystem getFileSystem(){
		FileSystem fs = null ;
		Configuration configuration = new Configuration();
		try {
			fs = FileSystem.get(configuration);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}
	public static void readFile(String pathStr){
		FileSystem fs = getFileSystem();
		FSDataInputStream fsis = null;
		try {
			 fsis = fs.open(new Path(pathStr));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			IOUtils.copyBytes(fsis, System.out, 4096, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String path ="/yxd/yarn";
		readFile(path);
		
	}

	
}
