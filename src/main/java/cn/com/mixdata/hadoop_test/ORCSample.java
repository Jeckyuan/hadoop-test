/**
 * @author Jeck
 *
 * Create on 2017年7月7日 上午11:46:28
 */
package cn.com.mixdata.hadoop_test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

public class ORCSample {
	private static Logger log = Logger.getLogger(ORCSample.class);

	public static class ORCMapper extends Mapper<NullWritable, OrcStruct, LongWritable, Text> {
		@Override
		public void map(NullWritable key, OrcStruct value, Context output) throws IOException, InterruptedException {
			int i = 10;
			if (i < 10) {
				log.info("device_devicetype=" + value.getFieldValue(73).toString());
				log.info("sign_requesttime=" + value.getFieldValue(11).toString());
				i++;
			}
			// output.write((Text) value.getFieldValue(1), (Text)
			// value.getFieldValue(2));
			LongWritable lk = new LongWritable(0L);
			String vl = "device_devicetype=" + value.getFieldValue(73).toString() + "\t" + "sign_requesttime="
					+ value.getFieldValue(11).toString();
			// output.write(lk, new Text(vl));
			output.write(new LongWritable(value.getNumFields()), new Text(value.toString()));
		}
	}

	public static class ORCReducer extends Reducer<Text, Text, NullWritable, OrcStruct> {
		private TypeDescription schema = TypeDescription.fromString("struct<name:string,mobile:string>");
		private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

		private final NullWritable nw = NullWritable.get();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
			for (Text val : values) {
				pair.setFieldValue(0, key);
				pair.setFieldValue(1, val);
				output.write(nw, pair);
			}
		}
	}

	public static void main(String args[]) throws Exception {

		Configuration conf = new Configuration();
		conf.set("orc.mapred.output.schema", "struct<name:string,mobile:string>");
		Job job = Job.getInstance(conf, "ORC Test");
		job.setJarByClass(ORCSample.class);
		job.setMapperClass(ORCMapper.class);
		// job.setReducerClass(ORCReducer.class);
		job.setInputFormatClass(OrcInputFormat.class);
		// job.setOutputFormatClass(OrcOutputFormat.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Text.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);
		// job.setOutputKeyClass(NullWritable.class);
		// job.setOutputValueClass(OrcStruct.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}