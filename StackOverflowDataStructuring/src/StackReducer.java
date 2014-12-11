


import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StackReducer extends Reducer<Label, LongWritable, Text, NullWritable> {

    private Text date = new Text();

    public void reduce(Label label, Iterable<LongWritable> values, Context context)

              throws IOException, InterruptedException {

    	long previous = 999999999999999999L;

    	Calendar calendar = new GregorianCalendar();

        //Find earliest known occurrence of key
        for (LongWritable value : values) {
        		if(value.get() < previous){
        			previous = value.get();
        		}
  
        }

        
        calendar.setTimeInMillis(previous);
        date.set(calendar.getTime().toString());
        context.write(new Text(label.toString()), NullWritable.get());

    }

}