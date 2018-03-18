package HeardFullyPackage;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HeardFullyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	  public void reduce(
			  IntWritable trackId,
			  Iterable<IntWritable> heardFull,
			  Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
		      throws IOException, InterruptedException {
		  
		  List<Integer> heardFullyList = new ArrayList<Integer>();
		  
		  for(IntWritable songsHeard: heardFull) {
			  
			  if(songsHeard.get() == 1) {
				  heardFullyList.add(songsHeard.get());
			  }
		  }
		  
		  IntWritable size = new IntWritable(heardFullyList.size());
		  context.write(trackId, size);
		  
	  }
}
