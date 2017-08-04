package cn.com.mixdata.hdfsUtil;
// cc MaxTemperatureMapperV4 Mapper for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

// vv MaxTemperatureMapperV4
public class MIMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static Logger log = Logger.getLogger(MIMapper.class);



    enum Temperature {
	MALFORMED
    }

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {

	parser.parse(value);
	if (parser.isValidTemperature()) {
	    int airTemperature = parser.getAirTemperature();
	    context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
	} else if (parser.isMalformedTemperature()) {
	    System.err.println("Ignoring possibly corrupt input: " + value);
	    context.getCounter(Temperature.MALFORMED).increment(1);
	}
    }



}
// ^^ MaxTemperatureMapperV4
