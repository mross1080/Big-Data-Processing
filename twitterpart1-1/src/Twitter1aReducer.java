

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Twitter1aReducer extends Reducer<IntIntPair, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    private Text keyAsString = new Text();

    public void reduce(IntIntPair key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;

        //Summation of all values of lengths in that range
        for (IntWritable value : values) {
        	sum += value.get();
  
        }

        result.set(sum);
        keyAsString.set(key.toString());       
        context.write(keyAsString, result);

    }

}