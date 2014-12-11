

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Lab4Mapper extends Mapper<Text, DailyStock, TextIntPair, DailyStock> { 

	TextIntPair pair = new TextIntPair();
	IntWritable year = new IntWritable();
    private Text data = new Text();
   // private StockInputFormat stock = new StockInputFormat();
    public void map(Text key, DailyStock stock, Context context) throws IOException, InterruptedException {
      
    	
    	Calendar calendar = new GregorianCalendar();
		calendar.setTimeInMillis(stock.getDay().get());
		calendar.getTimeInMillis();
		int calendarYear = calendar.get(Calendar.YEAR);
		
		year.set(calendarYear);
    	pair.set(stock.getCompany(),year);
    
    	
    	
        context.write(pair,stock);

//        }
    }
}
