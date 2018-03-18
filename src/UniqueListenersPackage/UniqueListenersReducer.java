package UniqueListenersPackage;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UniqueListenersReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	  public void reduce(
			  IntWritable trackId,
			  Iterable<IntWritable> userIds,
			  Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
		      throws IOException, InterruptedException {
		  
		  Set<Integer> userIdSet = new HashSet<Integer>();
	  		for (IntWritable userId : userIds) {
	  				userIdSet.add(userId.get());
	  		}
	  		IntWritable size = new IntWritable(userIdSet.size());
	  		context.write(trackId, size);
	  }
}
