

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StackMapper extends Mapper<Object, Text, Text, Label> { 


    private Text data = new Text();

    public void map(Object key, Text info, Context context) throws NullPointerException, IOException, InterruptedException {

    	// Format per label is 1229694	Ruby	[, xna, , ironruby]	Fri Sep 04 16:58:54 BST 2009


	    try {
	    	String [] fields = info.toString().split("::");
	    	String postId = fields[0];
	    	String family = fields[1];
	    	String tags = fields[2];
	    	String dateDump = fields[3];
	
	    	
	    	Label label= new Label();
	    	
	    	label.set(new Text(family), new Text(tags), new Text(postId), new LongWritable(Long.parseLong(dateDump.trim())));
	    	
	    	//Read in tech family and object 
	    	context.write(new Text(fields[1]), label);
	    	


	    } catch (NullPointerException e){
	    	 data.set("ERROR");
	    }
	      	    
}



}
