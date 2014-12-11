import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StocksReducer extends Reducer<Text, Iterable<NullWritable>, Text, NullWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<NullWritable> values, Context context)

              throws IOException, InterruptedException {

       

        for (NullWritable value : values) {

         

        }
               context.write(key, NullWritable.get());
             
    }

}

