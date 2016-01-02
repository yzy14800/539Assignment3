import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Plycount {

	public static class PlycountPartitioner extends Partitioner<IntWritable, IntWritable> {
		// Partition to the same reducer
		@Override
		public int getPartition(IntWritable arg0, IntWritable arg1, int arg2) {
			// TODO Auto-generated method stub
			return 0;
		}
	}

	public static class PlycountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			if (line.contains("[PlyCount")) {
				String[] tmp = line.split("\"");
				int plycnt = Integer.parseInt(tmp[1]);
				context.write(new IntWritable(plycnt), one);
			}
		}

	}

	public static class PlycountCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			context.write(key, new IntWritable(sum));
		}
	}

	public static class PlycountReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
		private int total = 0;
		private HashMap<Integer, Integer> frequency = new HashMap<Integer, Integer>();
		// key->freqCnt, value->Plycount

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			frequency.put(sum, key.get());
			total += sum;
		}

		@Override
		protected void cleanup(Reducer<IntWritable, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Object[] key = frequency.keySet().toArray();
			Arrays.sort(key);
			for (int i = key.length - 1; i >= 0; i--) {
				int plycnt = frequency.get(key[i]);
				double freq = (Integer) key[i] / (double) total * 100;
				context.write(new Text("" + plycnt + ""), new Text("" + freq + "%"));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(Plycount.class);
		job.setMapperClass(PlycountMapper.class);
		job.setCombinerClass(PlycountCombiner.class);
		job.setReducerClass(PlycountReducer.class);
		// specify a partitioner
		job.setPartitionerClass(PlycountPartitioner.class);

		// secify mapper output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// specify self-implemented InputFormatClass
		job.setInputFormatClass(WholeFileInputFormat.class);
		// TODO: specify input and output DIRECTORIES (not files)
		WholeFileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (!job.waitForCompletion(true))
			return;
	}

}
