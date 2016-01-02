import java.io.IOException;
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

public class Chess {
	public static class PGNPartitioner extends Partitioner<Text, IntWritable>{
		//Partition to the same reducer
		@Override
		public int getPartition(Text arg0, IntWritable arg1, int arg2) {
			// TODO Auto-generated method stub
			return 0;
		}
	}

	public static class PGNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text winner = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			if (line.contains("[Result")) {
				if (line.contains("0-1"))
					winner.set("Black");
				else if (line.contains("1-0"))
					winner.set("White");
				else if (line.contains("1/2-1/2"))
					winner.set("Draw");
				else
					return;
				context.write(winner, one);
			}
		}

	}

	public static class PGNCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			context.write(key,new IntWritable(sum));
		}

	}

	public static class PGNReducer extends Reducer<Text, IntWritable, Text, Text> {
		private int total = 0;
		private HashMap<String, Integer> percentage = new HashMap<String, Integer>();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			percentage.put(key.toString(), sum);
			total += sum;
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (String key : percentage.keySet()) {
				int cnt = percentage.get(key);
				String output = cnt + " " + (double) cnt / total;
				context.write(new Text(key), new Text(output));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(Chess.class);
		// TODO: specify a mapper
		job.setMapperClass(PGNMapper.class);
		 job.setCombinerClass(PGNCombiner.class);
		// TODO: specify a reducer
		job.setReducerClass(PGNReducer.class);
		//specify a partitioner
		job.setPartitionerClass(PGNPartitioner.class);
		
		// secify mapper output types
		job.setMapOutputKeyClass(Text.class);
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
