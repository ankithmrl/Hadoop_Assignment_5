package HeardFullyPackage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.*;

public class HeardFullyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	IntWritable trackId = new IntWritable();
    IntWritable heardFull = new IntWritable();
    
	public void map(LongWritable key, Text value, Context context)
			  throws IOException, InterruptedException {
		
		String record = value.toString();
		String parts[] = record.split("\\|");
		
		trackId.set(Integer.parseInt(parts[1]));
		heardFull.set(Integer.parseInt(parts[4]));
		
		if (parts.length == 5) {
			context.write(trackId, heardFull);
		}
	}

}