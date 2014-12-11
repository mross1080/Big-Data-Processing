

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Twitter1bReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;
        int count =0;


        for (IntWritable value : values) {
        	sum += value.get();
        	count++;
  
        }

               result.set(((double)sum/(double)count));
               
               context.write(key, result);

    }

}