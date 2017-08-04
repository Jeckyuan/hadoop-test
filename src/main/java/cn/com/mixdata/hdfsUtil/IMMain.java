/**
 * @author Jeck
 *
 * Create on 2017年8月4日 下午5:24:43
 */
package cn.com.mixdata.hdfsUtil;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author Jeck
 *
 * Create on 2017年8月4日 下午5:24:43
 */
public class IMMain extends Configured implements Tool {

    private static Logger log = Logger.getLogger(IMMain.class);

    @Override
    public int run(String[] args) throws Exception {
	if (args.length != 2) {
	    System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
	    log.error(String.format("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName()));
	    ToolRunner.printGenericCommandUsage(System.err);
	    return -1;
	}

	Job job = Job.getInstance(getConf(), "IMEI MAC maker");
	job.setJarByClass(getClass());

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setMapperClass(MIMapper.class);
	job.setCombinerClass(MIReducer.class);
	job.setReducerClass(MIReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	int exitCode = ToolRunner.run(new IMMain(), args);
	System.exit(exitCode);
    }
}
