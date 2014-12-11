

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class StackMapper extends Mapper<Object, Text, Label, LongWritable> { 

	private HashMap<String, String> countryCodes;
	
	
	 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    private LongWritable dateValue = new LongWritable();

    public void map(Object key, Text xmlString, Context context) throws NullPointerException, IOException, InterruptedException {
    	
    		ArrayList <String> labels = new ArrayList();
    		labels.add("java");
    		labels.add("python");
    		labels.add("c#");
    		labels.add("php");
    		labels.add("ruby");
    		labels.add("node.js");
    		labels.add("c++");
    		labels.add("perl");

    	    Map<String, String> parsed = MRDPUtils.transformXmlToMap(xmlString.toString());
    	
        	Calendar calendar = new GregorianCalendar();
        
    	    try {
    	    String info = parsed.get("Tags");
    	    String date = parsed.get("CreationDate");
    	    String id = parsed.get("Id");
    	    

            String [] tags = info.split(";");
            ArrayList<String> tagList = new ArrayList<String>(Arrays.asList(tags));
            
            Label label = new Label();

            String tag = "";
            for(int x =0; x < tagList.size(); x++){
            	tag = tagList.get(x);
            	//Remove the weird &alt thing
            	if(tag.substring(0, tag.length()-3) != " " && tag != "&lt"){
            		tagList.remove(x);
            		tagList.add(x, tag.substring(0, tag.length()-3));	
            	}
            	
            }
            LongWritable year= new LongWritable();
            //For every tag on this line
            for(String currentTag: tagList){
            	//Set what tech family it belongs in and emit that
        		if(labels.contains(currentTag)){
        			
                	calendar.set(Integer.parseInt(date.substring(0, 4)), Integer.parseInt(date.substring(5, 7)), Integer.parseInt(date.substring(8, 10)));
              		dateValue.set(calendar.getTimeInMillis());
              		year.set(calendar.getTimeInMillis());
              		label.set(new Text(currentTag), new Text(tagList.toString()), new Text(id), year);
              		
    			  context.write(label,label.getDate());
        			
        		}
        	}


    	    } catch (NullPointerException e){
//    	    	 data.set("ERROR");
//    	    	context.write(data, one);
    	    }
    	      	    
    }

}
