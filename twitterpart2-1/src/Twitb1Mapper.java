

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

import com.sun.corba.se.impl.ior.ByteBuffer;


public class Twitb1Mapper extends Mapper<Object, Text, Text, IntWritable> { 

    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	// Format per tweet is id;date;hashtags;tweet;
    	    String dump = value.toString();
    	    int startIndex;
    	    int endIndex;

    	    if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
    	        startIndex = StringUtils.ordinalIndexOf(dump,";",1) + 1;
    	        endIndex = StringUtils.ordinalIndexOf(dump,";",2) + 1;
    	        String fullDate = dump.substring(startIndex,endIndex);
    	        String parsedDate = fullDate.substring(0,10);
    	        
    	        data.set(parsedDate);
    	        context.write(data, one);
    	        	
    	      }
    	}
    
}
