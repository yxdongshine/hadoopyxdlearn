package hbase.yxd.high.aruq;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yxd.hadoop.webanalysis.SystemUtil;

public class HbbaseMapReduce extends Configured implements Tool{

	static final String FAIMILY_C = "info";
	static final String FAIMILY_COLL = "name";
	static final String FAIMILY_C_OTHER = "base";

	public static class hbaseMapper extends TableMapper<ImmutableBytesWritable, Put>{
		


		@Override
		protected void map(ImmutableBytesWritable key, Result result,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			Put outPut = new Put(key.get());
			for (int i = 0; i < result.rawCells().length; i++) {
				Cell cell = result.rawCells()[i];
				/*System.out.println(
						Bytes.toString(CellUtil.cloneRow(cell))
						+"------"+
						Bytes.toString(CellUtil.cloneFamily(cell))
						+"------"+
						Bytes.toString(CellUtil.cloneQualifier(cell))
						+"------"+
						Bytes.toString(CellUtil.cloneValue(cell))
						);*/
				// just get name coll 
				if(Bytes.toString(CellUtil.cloneFamily(cell)).equals(FAIMILY_C)){
					if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals(FAIMILY_COLL)){
						outPut.add(cell);
						 context.getCounter("looked","this right").increment(1L);
					}
				}
			}
			context.write(key, outPut);
		}
	}
	
	
	
	public static class hbaseReducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable>{
		
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Put> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
            for (Iterator iterator = values.iterator(); iterator.hasNext();) {
				Put put = (Put) iterator.next();
				List<Cell> cellList =put.get(Bytes.toBytes(FAIMILY_C),Bytes.toBytes(FAIMILY_COLL));
				if(cellList!=null){
					 context.getCounter("write","start ").increment(1L);

					for (Iterator iterator2 = cellList.iterator(); iterator2
							.hasNext();) {
						Cell cell = (Cell) iterator2.next();
						Put baseNamePut  = new Put(key.get());
						baseNamePut.add(Bytes.toBytes(FAIMILY_C_OTHER),CellUtil.cloneQualifier(cell),CellUtil.cloneValue(cell));
                        context.write(key, baseNamePut);
					}
				}
			}
		}
	}
	
	
	
	
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "HbbaseMapReduce");
		job.setJarByClass(HbbaseMapReduce.class);

		
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("student:stu_info", scan, hbaseMapper.class, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob("student:back_stu_info", hbaseReducer.class, job);
		
		job.setNumReduceTasks(1);
		
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0:1;
	}

	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();

		int status = ToolRunner.run(conf, new HbbaseMapReduce(), args);

		//query student:back_stu_info
		HbaseClient.scanTable("student:back_stu_info");
		
		System.exit(status);
	}
}
