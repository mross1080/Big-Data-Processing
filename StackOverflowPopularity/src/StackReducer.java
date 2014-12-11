

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StackReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text label, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;
        int count =0;
        
        for (IntWritable value : values) {
        	  sum += value.get();
        	  count++;
        		
        }

        context.write(new Text("For " + label.toString() + " number of posts is: " + count + " and total score is: "), new IntWritable(sum));

    }

}