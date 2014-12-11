

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class StockCompanyReplJoinMapper extends Mapper<Text, DailyStock, TextIntPair, IntWritable> {

	  private Hashtable<String, String> companyInfo;
	  private Text companySector = new Text();
	  private IntWritable year = new IntWritable();
	  private TextIntPair pair = new TextIntPair();

	  @Override
	  protected void setup(Context context) throws IOException, InterruptedException {
	    companyInfo = new Hashtable<String, String>();
	    // We know there is only one cache file, so we only retrieve that URI
	    URI fileUri = context.getCacheFiles()[0];
	    FileSystem fs = FileSystem.get(context.getConfiguration());
	    FSDataInputStream in = fs.open( new Path(fileUri) );
	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String line = null;
	    try {
	     // we discard the header row
	      br.readLine();
	      while ((line = br.readLine()) != null) {
	        context.getCounter(CustomCounters.NUM_COMPANIES).increment(1);
	        String[] fields = line.split("\t");
	        // Fields are: 0:Symbol 1:Name 2:IPOyear 3:Sector 4:industry 
	        if (fields.length == 5)
	          companyInfo.put(fields[0], fields[3]);
	      }
	      br.close();
	     } catch (IOException e1) {
	     }
	   super.setup(context);
	   }
	   
	   public void map(Text key, DailyStock stock, Context context) throws IOException, InterruptedException {
		   
		   Calendar calendar = new GregorianCalendar();
			calendar.setTimeInMillis(stock.getDay().get());
			calendar.getTimeInMillis();
			int calendarYear = calendar.get(Calendar.YEAR);
			
			year.set(calendarYear);
	    	pair.set(stock.getCompany(),year);
		   
		   companySector.set(companyInfo.get(stock.getCompany().toString()));
		   
		   pair.set(companySector, year);
		   		   
		   
		   
		   context.write(pair,stock.getVolume());
		   
		   
		   
		   
	}
	}