
package hbase.yxd.high.aruq;  

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


/** 
 * ClassName:HbaseClient <br/> 
 * Function: TODO (hbase的增删改查api简单的使用). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2016年12月13日 下午2:35:58 <br/> 
 * @author   yxd 
 * @version   
 * @see       
 */
public class HbaseClient {
	

	/**
	 * 
	 * getTable:(). <br/> 
	 * TODO(根据名称获取数据表).<br/> 
	 * 
	 * @author yxd 
	 * @param tableName
	 * @return
	 */
	public static HTable getTable(String tableName){
		Configuration conf = HBaseConfiguration.create();
		
		HTable htable = null;
		try {
			htable = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return htable;
	}
	
	/**
	 * 
	 * scanTable:(). <br/> 
	 * TODO(获取表数据).<br/> 
	 * 
	 * @author yxd 
	 * @param tableName
	 */
	public static void scanTable(String tableName){
		HTable htable = getTable(tableName);
		if(htable !=null){
			Scan scan = new Scan();
			ResultScanner rs = null;
			try {
				rs = htable.getScanner(scan);
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(rs!=null){
				for (Iterator iterator = rs.iterator(); iterator
						.hasNext();) {
					Result result = (Result) iterator.next();
					for (int i = 0; i < result.rawCells().length; i++) {
						Cell cell = result.rawCells()[i];
						System.out.println(
								Bytes.toString(CellUtil.cloneRow(cell))
								+"------"+
								Bytes.toString(CellUtil.cloneFamily(cell))
								+"------"+
								Bytes.toString(CellUtil.cloneQualifier(cell))
								+"------"+
								Bytes.toString(CellUtil.cloneValue(cell))
								);
					}
				}
			}
		}
	}
	
	public static void main(String[] args) {
		scanTable("student:stu_info");
	}
	
}
  