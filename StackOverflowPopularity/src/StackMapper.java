

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StackMapper extends Mapper<Object, Text, Text, IntWritable> { 

    public void map(Object key, Text xmlString, Context context) throws NullPointerException, IOException, InterruptedException {

		ArrayList <String> labels = new ArrayList();
		labels.add("java");
		labels.add("python");
		labels.add("c#");
		labels.add("php");
		labels.add("ruby");
		labels.add("node.js");
		 	
	    Map<String, String> parsed = MRDPUtils.transformXmlToMap(xmlString.toString());
	    

	    try {
	    String tagsAsString = parsed.get("Tags");
	    int score = Integer.parseInt(parsed.get("Score").trim());
	    
        String [] tags = tagsAsString.split(";");
        ArrayList<String> tagList = new ArrayList<String>(Arrays.asList(tags));

        String tag = "";
        for(int x =0; x < tagList.size(); x++){
        	tag = tagList.get(x);
        	//Remove the weird &alt thing
        	if(tag.substring(0, tag.length()-3) != " " && tag != "&lt"){
        		tagList.remove(x);
        		tagList.add(x, tag.substring(0, tag.length()-3));	
  
        	}
        	
        }
        
        //For every tag on this line
        for(String currentTag: tagList){
        	//Set what tech family it belongs in and emit that
    		if(labels.contains(currentTag)){
			  context.write(new Text(currentTag), new IntWritable(score));
    			
    		}
    	}

       
	    } catch (NullPointerException e){
//	    	 data.set("ERROR");
//	    	context.write(data, one);
	    }
	      	    
    }

}
