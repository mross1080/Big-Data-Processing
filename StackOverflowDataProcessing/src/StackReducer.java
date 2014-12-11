

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class StackReducer extends Reducer<Text, Label, Text, IntWritable> {

    HashMap <Long, String> yearValues;
    
    protected void setup(Context context) throws IOException, InterruptedException {
    	yearValues = new  HashMap <Long, String>();
        URI fileUri = context.getCacheFiles()[0];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open( new Path(fileUri) );
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
        	
          br.readLine();
          //Read in year and millis values
          while ((line = br.readLine()) != null) {
        	  String [] arr= line.split("::");
        	  yearValues.put(Long.parseLong(arr[1].trim()), arr[0]);
        	  
          }
          br.close();
         } catch (IOException e1) {
         }
       super.setup(context);
       }

    public void reduce(Text label, Iterable<Label> values, Context context)

              throws IOException, InterruptedException {

        HashMap <String, ArrayList<Label>> yearInformation = new HashMap<String,ArrayList<Label>>();
        ArrayList<Long> yearIndex = new ArrayList<Long>();
        
        //Hashmap which has <Year,<List<Label>>
        //year index is the array which holds all the years>
        for(Long year: yearValues.keySet()){
        	yearInformation.put(yearValues.get(year), new ArrayList<Label>());
        	yearIndex.add(year);
        }

        String currentYear;
        //For every label
        for (Label value : values) {
        	//Find what year it belongs in
        	//Find year uses binary search to find the correct year
        	// yearIndex uses the index of the year to find the millisecond value of the correct year
        	//year values gets the actual year in integer
        		currentYear = yearValues.get(yearIndex.get(findYear(value,yearIndex)));
        		//Add that to the hashmap that has all labels for that year
        		yearInformation.get(currentYear).add(value);	
        		  
        }
        
        int languageCount =0;
        for(String year: yearInformation.keySet()){
        	languageCount += yearInformation.get(year).size();
        	
        	context.write(new Text("The number of groupings of technology in" + year + "increased by "), new IntWritable(languageCount));
        }

    }
    
    public static int findYear(Label current, ArrayList<Long> yearIndex){
    	long date = current.getDate().get();
    	int low= 0;
          int high = yearIndex.size();
          while(low< high){
          	int mid = low + (high-low) /2;
          	if(date < yearIndex.get(mid)) high = mid -1;
          	else if (date > yearIndex.get(mid)) low = mid +1;
          	else return mid;
          }
          
          return 0;
    	
    	
    }

}