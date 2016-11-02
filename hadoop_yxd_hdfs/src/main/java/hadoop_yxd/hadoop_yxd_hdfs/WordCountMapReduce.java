package hadoop_yxd.hadoop_yxd_hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapReduce {

	// step 1 : Mapper Class
	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		// 输出单词
		private Text mapOutputKey = new Text();
		// 出现一次就记做1次
		private IntWritable mapOutputValue = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("map-in-0-key: " + key.get() + " -- "
					+ "map-in-value: " + value.toString());

			// line value
			// 获取文件每一行的<key,value>
			String lineValue = value.toString();

			// split
			// 分割单词，以空格分割
			String[] strs = lineValue.split(" ");

			// iterator
			// 将数组里面的每一个单词拿出来，一个个组成<key,value>
			// 生成1
			for (String str : strs) {
				// 设置key
				// set map output key
				mapOutputKey.set(str);

				// output
				// 最终输出
				context.write(mapOutputKey, mapOutputValue);
			}
		}

	}

	// step 2: Reducer Class
	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// temp : sum
			// 定义一个临时变量
			int sum = 0;

			// iterator
			// 对于迭代器中的值进行迭代累加，最后sum加完以后就是统计的次数
			for (IntWritable value : values) {
				// total
				sum += value.get();
			}

			// set output value
			outputValue.set(sum);

			// output
			context.write(key, outputValue);
		}
	}

	// step 3: Driver
	public int run(String[] args) throws Exception {

		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration, this.getClass()
				.getSimpleName());
		job.setJarByClass(WordCountMapReduce.class);

		// set job
		// input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);

		// output
		Path outPath = new Path(args[1]);
		FileSystem fs = getFileSystem();
		if(fs.exists(outPath)){
			//delete
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		// Mapper
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Reducer
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	
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
	public static void main(String[] args) throws Exception {

		// 传递两个参数，设置路径
		args = new String[] {
				"hdfs://hadoop1:9000/yxd/yarn",
				"hdfs://hadoop1:9000/yxd/test5" };

		// run job
		int status = new WordCountMapReduce().run(args);

		System.exit(status);
	}
}
