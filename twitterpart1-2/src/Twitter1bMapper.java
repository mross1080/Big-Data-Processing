

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

import com.sun.corba.se.impl.ior.ByteBuffer;


//public class lab2Mapper extends Mapper<Object, Text, IntWritable, IntWritable> { 
public class Twitter1bMapper extends Mapper<Object, Text, Text, IntWritable> { 


	 
    private final IntWritable one = new IntWritable(1);
    private IntWritable result = new IntWritable();
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	// Format per tweet is id;date;hashtags;tweet;
    	    String dump = value.toString();
    	    int startIndex;
    	    //Improve this
    	    //Citing http://www.rgagnon.com/javadetails/java-0536.html
    	    CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
    	    
    	    
    	    
    	    if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
    	        startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
    	        String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
    	        
    	        if(asciiEncoder.canEncode(tweet)){
    	        	if(checkForAscii(tweet)){
    	        		result.set(tweet.length());
    	        data.set("Average");
    	        context.write(data, result);
    	        	}
    	        }
    	      }
    	}
    
    public static boolean checkForAscii(String tweet) {
    	for (char c: tweet.toCharArray()){
    		  if (((int)c)>127){
    		    return false;
    		  } 
    		}
    		return true;	 
    		    
    		}
}
