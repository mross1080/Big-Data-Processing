

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;




public class Twitter1aMapper extends Mapper<Object, Text, IntIntPair, IntWritable> { 
 
	 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    private IntIntPair pair = new IntIntPair();
    private IntWritable lowInt = new IntWritable();
    private IntWritable highInt = new IntWritable();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	// Format per tweet is id;date;hashtags;tweet;
    	    String dump = value.toString();
    	    int startIndex;
    	    
    	    //Encoder to check if ASCII value
    	    CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder(); 
    	    
    	    //Create Array to hold values of all multiples of 5 up to 140
    	    int [] highs = new int[140/5];
    	    int count =0;
            for(int x =0; x< highs.length; x++){
    			
    			count +=5;
    			highs[x] = count;
            }
    			
    	    
    	    if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
    	        startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
    	        String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
    	        
    	        //Check if tweet is a valid string
    	        if(encoder.canEncode(tweet)){
    	        	if(checkForAscii(tweet) && (tweet.length() <= 140) ){
    	        		
    	        		//loop through array and find what range tweet length is in
    	                for(int x =0; x< highs.length; x++){
    	        			int low = highs[x]-5;
    	        			int high = highs[x];
    	        			if(tweet.length() > low && tweet.length() < high){
    	        			
    	        				 lowInt.set(low);
    	        				 highInt.set(high);
    	        				 pair.set(lowInt, highInt);
    	        				 //Emit low and high range as intintpair and value of 1
    	        	    	     context.write(pair, one);
    	        				
    	        			}

    	        	}
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
