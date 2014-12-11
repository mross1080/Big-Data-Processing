

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StackMapper extends Mapper<Object, Text, Text, IntWritable> { 
	 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    ArrayList<String> vals= new ArrayList<String>();


    public void map(Object key, Text xmlString, Context context) throws NullPointerException, IOException, InterruptedException {


    	    Map<String, String> parsed = MRDPUtils.transformXmlToMap(xmlString.toString());
    	    int vote;
   	    
  	    	try{
    	    	vote = Integer.parseInt(parsed.get("VoteTypeId").trim());
   	    	if(vote == 2 || vote == 1 ){
    	    		context.write(new Text(parsed.get("PostId").trim()), new IntWritable(1));
 	    	} 
 	    	
 	    } catch (NullPointerException e){
  	    	 data.set("ERROR");
  	    	context.write(data, one);   	   
  	    	
 	    }

        	   
    }
    
}
