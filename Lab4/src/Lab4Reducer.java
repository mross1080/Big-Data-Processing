

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Lab4Reducer extends Reducer<TextIntPair, DailyStock, Text, Text> {

	
	Text key = new Text();
	Text value = new Text();
    public void reduce(TextIntPair pair, Iterable<DailyStock> stocks, Context context)

              throws IOException, InterruptedException {

    	double min= 99999999999999.9;
    	double max= 0.0;
    	int volume= 0;
    	
    	for(DailyStock stock: stocks){
    		if(stock.getLow().get() < min){
    			min = stock.getLow().get();
    		}
    		
    		if(stock.getHigh().get() > max){
    			max = stock.getHigh().get();
    		}
    	
    		volume += stock.getVolume().get();
    	}
    	
    	
    	key.set(pair.toString());
    	value.set(min + "," + max + "," + volume );
    	context.write(key, value );
    		

    }

}