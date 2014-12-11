import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StocksMapper extends Mapper<LongWritable, Text, Text, NullWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
    	int count =0;
    	String a = "";
        StringTokenizer itr = new StringTokenizer(line.toString(), "-- \t\n\r\f,.:;?![]'\"");
        
        String [] names = line.toString().split(",");
        data.set(names[1]);
        while (itr.hasMoreTokens()) {
            data.set(itr.nextToken().toLowerCase());
            context.write(data, NullWritable.get());
            
            
          }
        //context.write(data, NullWritable.get());
        
        
    }
}
