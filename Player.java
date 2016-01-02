import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Player {

	public static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable() {
			super(IntWritable.class);
		}
	}

	public static class PlayerMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
		private final static IntArrayWritable result = new IntArrayWritable();
		private Text white = new Text();
		private Text black = new Text();
		private IntWritable one = new IntWritable(1);
		private IntWritable zero = new IntWritable(0);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			if (line.contains("[White ")) {
				String[] temp = line.split("\"");
				white.set(temp[1]);
				return;
			}
			if (line.contains("[Black ")) {
				String[] temp = line.split("\"");
				black.set(temp[1]);
				return;
			}
			if (line.contains("[Result")) {
				IntWritable[] bwin = { one, zero, zero, zero };
				IntWritable[] blose = { zero, one, zero, zero };
				IntWritable[] bdraw = { zero, zero, one, zero};
				IntWritable[] wwin = { one, zero, zero, one };
				IntWritable[] wlose = { zero, one, zero, one };
				IntWritable[] wdraw = { zero, zero, one, one};
				if (line.contains("0-1")) {
					result.set(bwin);
					context.write(black, result);
					result.set(wlose);
					context.write(white, result);
				} else if (line.contains("1-0")) {
					result.set(wwin);
					context.write(white, result);
					result.set(blose);
					context.write(black, result);
				} else if (line.contains("1/2-1/2")) {
					result.set(bdraw);
					context.write(black, result);
					result.set(wdraw);
					context.write(white, result);
				} else {
					return;
				}
			}
		}

	}

	public static class PlayerCombiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int bwin = 0, blose = 0, bdraw = 0;
		    int wwin = 0, wlose = 0, wdraw = 0;
			int role=0; //0->black, 1->white
			for (IntArrayWritable r : values) {
				IntWritable[] tmp = (IntWritable[]) r.toArray();
				role =tmp[3].get();
				if(role==0){
					bwin += tmp[0].get();
					blose += tmp[1].get();
					bdraw += tmp[2].get();
				}else{
					wwin += tmp[0].get();
					wlose += tmp[1].get();
					wdraw += tmp[2].get();
				}
			}
			IntArrayWritable result=new IntArrayWritable();
			//send black
			IntWritable[] btmp={new IntWritable(bwin),new IntWritable(blose),new IntWritable(bdraw),new IntWritable(0)};
			result.set(btmp);
			context.write(key, result);
			//send white
			IntWritable[] wtmp={new IntWritable(wwin),new IntWritable(wlose),new IntWritable(wdraw),new IntWritable(1)};
			result.set(wtmp);
			context.write(key, result);
		}
	}

	public static class PlayerReducer extends Reducer<Text, IntArrayWritable, Text, Text> {
		private Text result = new Text();
		@Override
		protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int bwin = 0, blose = 0, bdraw = 0;
		    int wwin = 0, wlose = 0, wdraw = 0;
			int role=0; //0->black, 1->white
			for (IntArrayWritable r : values) {
				IntWritable[] tmp = (IntWritable[]) r.toArray();
				role =tmp[3].get();
				if(role==0){
					bwin += tmp[0].get();
					blose += tmp[1].get();
					bdraw += tmp[2].get();
				}else{
					wwin += tmp[0].get();
					wlose += tmp[1].get();
					wdraw += tmp[2].get();
				}
			}
			//send black
			double total = bwin + blose + bdraw;
			String player=key.toString()+" Black";
			result.set(bwin / total + " " + blose / total + " " + bdraw / total);
			if(total==0)
				result.set("0.0, 0.0 0.0");
			context.write(new Text(player), result);
			//send white
			total = wwin + wlose + wdraw;
			player=key.toString()+" White";
			result.set(wwin / total + " " + wlose / total + " " + wdraw / total);
			if(total==0)
				result.set("0.0, 0.0 0.0");
			context.write(new Text(player), result);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(Player.class);
		job.setMapperClass(PlayerMapper.class);
		job.setCombinerClass(PlayerCombiner.class);
		job.setReducerClass(PlayerReducer.class);

		// secify mapper output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);

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
