package SongSharedPackage;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SongSharedReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	  public void reduce(
			  IntWritable trackId,
			  Iterable<IntWritable> songShared,
			  Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
		      throws IOException, InterruptedException {
		  
		  List<Integer> songSharedList = new ArrayList<Integer>();
		  
		  for(IntWritable shared: songShared) {
			  
			  if(shared.get() == 1) {
				  songSharedList.add(shared.get());
			  }
		  }
		  
		  IntWritable size = new IntWritable(songSharedList.size());
		  context.write(trackId, size);
		  
	  }
}
