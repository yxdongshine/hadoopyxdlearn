
package hbase.yxd.high.aruq;  

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
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
	
	
	/**
	 * 
	 * add:(). <br/> 
	 * TODO().<br/> 
	 * 
	 * @author yxd 
	 * @param tableName 表名
	 * @param rowKey 键
	 * @param faiC 列簇
	 * @param ce 列明
	 * @param value 值
	 */
	public static void add(String tableName,String rowKey,String faiC,String ce ,String value){
		HTable htable =getTable(tableName);
		if(htable!=null){
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(faiC), Bytes.toBytes(ce), Bytes.toBytes(value));
			try {
				htable.put(put);
			} catch (RetriesExhaustedWithDetailsException e) {
				e.printStackTrace();
			} catch (InterruptedIOException e) {
				e.printStackTrace();
			}
		}

	} 

	/**
	 * 
	 * delete:(). <br/> 
	 * TODO().<br/> 
	 * 
	 * @author yxd 
	 * @param tableName 表名
	 * @param rowKey 键
	 * @param faiC 列簇
	 * @param ce 列明

	 */
	public static void delete(String tableName,String rowKey,String faiC,String ce){
		HTable htable =getTable(tableName);
		if(htable!=null){
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.deleteColumn(Bytes.toBytes(faiC), Bytes.toBytes(ce));
				try {
					htable.delete(delete);
				} catch (IOException e) {
					e.printStackTrace();
				}
			
		}
	}
	
	public static void conitionScan(String tableName,String perfixStgr){
		HTable htable = getTable(tableName);
		if(htable !=null){
			Scan scan = new Scan();
			Filter filter = new PrefixFilter(Bytes.toBytes(perfixStgr));
			scan.setFilter(filter);
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
	
	private static final String TABLE_NAME = "student:stu_info";
	public static void main(String[] args) {
		/**
		 * add
		 */
		String rowKey = "1002";
		String faiC = "info";
		String ce = "info:name";
		String value = "yangxiaodong";

		//add(TABLE_NAME, rowKey, faiC, ce, value);
		
		/**
		 * delete
		 */
		//delete(TABLE_NAME, rowKey, faiC, ce);
		/**
		 * 获取数据
		 */
		//scanTable(TABLE_NAME);
		/**
		 * 高级查询
		 */
		String perfixStr="1002";
		conitionScan(TABLE_NAME, perfixStr);
	}
	
}
  