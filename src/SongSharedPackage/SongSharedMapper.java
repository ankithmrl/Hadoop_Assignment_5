package SongSharedPackage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.*;

public class SongSharedMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	IntWritable trackId = new IntWritable();
    IntWritable songShared = new IntWritable();
    
	public void map(LongWritable key, Text value, Context context)
			  throws IOException, InterruptedException {
		
		String record = value.toString();
		String parts[] = record.split("\\|");
		
		trackId.set(Integer.parseInt(parts[1]));
		songShared.set(Integer.parseInt(parts[2]));
		
		if (parts.length == 5) {
			context.write(trackId, songShared);
		}
	}

}