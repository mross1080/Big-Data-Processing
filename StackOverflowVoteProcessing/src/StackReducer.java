

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StackReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private Text data = new Text();

    public void reduce(Text id, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
        		
        	sum += value.get();
        		
        }
        
        //Emit PostId and total number of upvotes and downvotes
        data.set(id.toString() + "::" + sum);
        context.write(data, NullWritable.get());

    }

}