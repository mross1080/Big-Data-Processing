

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class lab5Reducer extends Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterable<NullWritable> values, Context context)

              throws IOException, InterruptedException {

               //context.write(key, NullWritable.get());

    }

}