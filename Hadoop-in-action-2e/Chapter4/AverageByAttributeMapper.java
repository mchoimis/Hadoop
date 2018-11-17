import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageByAttributeMapper extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		static enum ClaimsCounters {
			MISSING, QUOTED
		};

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String fields[] = value.toString().split(",", -20);
			String country = fields[4];
			String numClaims = fields[8];

			if (numClaims.length() > 0 && !numClaims.startsWith("\"")) {
			    context.write(new Text(country),
					  new Text(numClaims + ",1"));
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, DoubleWritable> {

		public void reduce(Text key, Iterator<Text> values,
				Context context) throws IOException, InterruptedException {

			double sum = 0;
	        int count = 0;
	        while (values.hasNext()) {
	            String fields[] = values.next().toString().split(",");
	            sum += Double.parseDouble(fields[0]);
	            count += Integer.parseInt(fields[1]);
	        }
	        context.write(key, new DoubleWritable(sum/count));

		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "average");

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(AverageByAttributeMapper.class);
		job.setJobName("Average");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);	

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new AverageByAttributeMapper(), args);

		System.exit(res);
	}
}
