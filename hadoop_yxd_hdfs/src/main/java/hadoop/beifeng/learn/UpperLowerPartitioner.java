package hadoop.beifeng.learn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UpperLowerPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {

		String str = key.toString();

		if (str.substring(0, 1).matches("A-Z")) {
			return 0;
		}

		return 1;
	}

}
