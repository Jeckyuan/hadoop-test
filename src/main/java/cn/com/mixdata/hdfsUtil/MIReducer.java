package cn.com.mixdata.hdfsUtil;

//cc MaxTemperatureReducerV1 Reducer for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

// vv MaxTemperatureReducerV1
public class MIReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private static Logger log = Logger.getLogger(MIReducer.class);

    private String imeiOutputPath = null;
    private String macOutputPath = null;
    private MultipleOutputs<Text, NullWritable> multipleOutputs;
    private Long max = Long.parseLong("FFFFFF", 16);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {

	int maxValue = Integer.MIN_VALUE;
	for (IntWritable value : values) {
	    maxValue = Math.max(maxValue, value.get());
	}
	context.write(key, NullWritable.get());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	imeiOutputPath = conf.get("ImeiOutputPath");
	macOutputPath = conf.get("MacOutputPath");

	if (imeiOutputPath == null || macOutputPath == null) {
	    log.error("输出路径未设置：");
	    log.error("ImeiOutputPath=" + imeiOutputPath);
	    log.error("MacOutputPath=" + macOutputPath);
	    System.exit(1);
	}

	multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
    }

    private Boolean writeAllImei(String key) throws IOException, InterruptedException {

	String[] keySplit = key.split("_");
	if (keySplit.length == 2 && keySplit[1].trim().length() == 6) {
	    String imeiPrefix = keySplit[1].trim();
	    if (imeiPrefix.trim().length() == 6) {
		for (Long ind = 0L; ind < 1000000L; ind++) {
		    String imei = imeiPrefix + String.format("%1$06d", ind);
		    multipleOutputs.write(new Text(imei), NullWritable.get(), imeiOutputPath);
		}
		return Boolean.TRUE;
	    } else {
		log.error("该IMEI前缀格式不正确：" + imeiPrefix);
		return Boolean.FALSE;
	    }
	} else {
	    log.error("该IMEI前缀格式不正确：" + key);
	    return Boolean.FALSE;
	}
    }


    private Boolean writeAllMac(String key) throws IOException, InterruptedException {

	String[] keySplit = key.split("_");
	if (keySplit.length == 2 && keySplit[1].trim().length() == 6) {
	    String macPrefix = keySplit[1].trim();
	    if (macPrefix.trim().length() == 6) {
		for (Long ind = 0L; ind <= max; ind++) {
		    String mac = macPrefix + String.format("%1$06x", ind);
		    multipleOutputs.write(new Text(mac), NullWritable.get(), imeiOutputPath);
		}
		return Boolean.TRUE;
	    } else {
		log.error("该MAC前缀格式不正确：" + macPrefix);
		return Boolean.FALSE;
	    }
	} else {
	    log.error("该MAC前缀格式不正确：" + key);
	    return Boolean.FALSE;
	}
    }

}
// ^^ MaxTemperatureReducerV1
