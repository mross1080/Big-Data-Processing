 
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.*;
import java.net.URL;
import java.nio.file.Paths;
import java.io.*;

import org.apache.hadoop.io.Text;
 
/* Name of the class has to be "Main" only if the class is public. */
class test
{
public static void main (String[] args) throws java.lang.Exception {
	
	
	
	
	
}
	
	
	
	
	 public static int findYear(long current, ArrayList<Long> yearIndex){
	    	long date = current;
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
//	String str = "  <row Id=\"8717171\" UserId=\"1872432\" Name=\"Yearling\" Date=\"2014-01-19T05:11:46.703\" />";
//	Map <String, String> map = transformXmlToMap(str);
//	map.put(map.get("Name"), "Hello");
//	System.out.println(map.get("Name"));
//	int [] highs = new int[180/5];
//	int count =0;
////	for(int x =0; x< highs.length; x++){
////		count += 5;
////		highs[x] = count;
////		System.out.println(highs[x]);
////		
//	}
//	ArrayList<String> finalRecommendations = new ArrayList<String>();
//	ArrayList<String> arr1 = new ArrayList<String>();
//	ArrayList<String> arr2 = new ArrayList<String>();
//	ArrayList<String> arr3 = new ArrayList<String>();
//	ArrayList<ArrayList> arrs = new ArrayList<ArrayList>();
//	
//
//
//	
//	arr1.add("ruby");
//	arr1.add("javascript");
//	arr1.add("haml");
//	arr2.add("ruby");
//	arr2.add("javascript");
//	arr2.add("jquery");
//	arr2.add("haml");
//	arr2.add("heroku");
//	arr3.add("ruby");
//	arr3.add("rspec");
//	arr3.add("cucumber");
//	
//	String [] movies;
//
//    for (String recs : stuff) {
//    	movies= recs.toString().split(",");
//    	for(String str: movies){
//    		if(!finalRecommendations.contains(str)){
//    			finalRecommendations.add(str);
//    		}
//    	} 
//    	
//
//    }
//    
//    System.out.println(finalRecommendations.size());
//	
//	
//	
//	HashMap<Integer, String> hash = new HashMap();
//	
//	String str = "<row Id=\"1123\" PostId=\"122\" VoteTypeId=\"2\" CreationDate=\"2008-07-31T00:00:00.000\" />";
//	String str2 = "Hello";
//	
//	hash.put(1123, str);
//	hash.put(1144, str2);
////	System.out.println(hash.get(1123));
//	
//	for(Integer key: hash.keySet()){
//		System.out.print(hash.get(key));
////	}
//	
//	
//	
//
//		System.out.println(hash.toString());
//	
//		HashMap<String, String> votes = new HashMap<String,String>();
//	  MRDPUtils parser = new MRDPUtils();
//	  Map<String, String> parsed;
//	  
//	  parsed = (HashMap<String, String>) MRDPUtils.transformXmlToMap(str);
//	  votes.put(parsed.get("Id"), parsed.toString());
//	  System.out.println(parsed.size());
//
////	 String[] fields = line.split("\t");
     // Fields are: 0:Symbol 1:Name 2:IPOyear 3:Sector 4:industry 
//     if (fields.length == 5)
//       votes.put(fields[0], fields.toString());
//   }


//for(String vote: votes.keySet()){
//	 parsed = MRDPUtils.transformXmlToMap(votes.get(vote));
//	 id.set(vote);
//	 data.set(parsed.toString());
//	 context.write(id, data);
//	 
//	}

//	URL url = new URL("https://rubygems.org/api/v1/search.json?query=cucumber");
//
//	try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
//	    for (String line; (line = reader.readLine()) != null;) {
//	        System.out.println(line);
//	    }
//	}


//	System.out.println(midd);
	
//	ArrayList<String> years = new ArrayList<String>();
//	
//	String date1 = "2008-01-01T03:45:25.263";
//	String date2 = "2009-01-01T03:45:25.263";
//	String date3 ="2010-01-01T03:45:25.263";
//	String date4 = "2011-01-01T03:45:25.263";
//	String date5 = "2012-01-01T03:45:25.263";
//	String date6 = "2013-01-01T03:45:25.263";
//	String date7 = "2014-01-01T03:45:25.263";
//	
//	years.add(date1);
//	years.add(date2);
//	years.add(date3);
//	years.add(date4);
//	years.add(date5);
//	years.add(date6);
//	years.add(date7);
//	
//	Calendar calendar = new GregorianCalendar();
//	
//	int start = 2008;
//	for(String date: years){
//		calendar.set(Integer.parseInt(date.substring(0, 4)), Integer.parseInt(date.substring(5, 7)), Integer.parseInt(date.substring(8, 10)));
//		System.out.println(start + "::" + calendar.getTimeInMillis());
//		start++;
//	}
//	
//	
////	String j= "javascript";
//
//	Calendar calendar = new GregorianCalendar();
//	Calendar previous= new GregorianCalendar();
////	calendar.set(1993, 05, 28);
////	previous.set(2013, 02, 11);
//	System.out.print(date1.substring(8, 10));
//	 
//	
////	
//	calendar.set(Integer.parseInt(date1.substring(0, 4)), Integer.parseInt(date1.substring(5, 7)), Integer.parseInt(date1.substring(8, 10)));
//	previous.set(Integer.parseInt(date2.substring(0, 4)), Integer.parseInt(date2.substring(5, 7)), Integer.parseInt(date2.substring(8, 10)));
//	System.out.println("2008 in long is " + previous.getTimeInMillis());
////
//	System.out.println(calendar.getTimeInMillis());
	//	previous.setTimeInMillis());
//	calendar.setTimeInMillis();
//	calendar.getTimeInMillis();

	
    public static Map<String, String> transformXmlToMap(String xml) {
    	Map<String, String> map = new HashMap<String, String>();
    	try {
    	String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
    	.split("\"");
    	for (int i = 0; i < tokens.length - 1; i += 2) {
    	String key = tokens[i].trim();
    	String val = tokens[i + 1];
    	map.put(key.substring(0, key.length() - 1), val);
    	}
    	} catch (StringIndexOutOfBoundsException e) {
    	System.err.println(xml);
    	}
    	return map;
    	}
	
	
}
//	String input = 
//            "User clientId=23421. teamasdfsadf Some more text clientId=33432. This clientNum=100";
//	String text = "#teamUSA goUSA #teamUK #goUS gorinaldo teamlondon actuallythatoneiskindoftrue";
//	Pattern p = Pattern.compile("(#go|#team)(\\w+)");
//	Matcher m = p.matcher(text);
//
//	StringBuffer result = new StringBuffer();
//	while (m.find()) {
//		System.out.println("Found Coutnry: " + m.group(2));
//		m.appendReplacement(result, m.group(1) + "***masked***");
//	}
//	m.appendTail(result);
//	System.out.println(result);
//	
//	
	
	
// your code goes here
	//String text = "6xGold  TeamGB goUSA London2012 ";
	//String text = "teamUSA goUSA teamUK goUS gorinaldo teamlondon actuallythatoneiskindoftrue";
	//String text = "I lost my wallet";
	//String pattern = "I lost my \\w+";
//   String pattern = "Team.|go";
//	//String pattern = "team+\\S|go..\\S|team.\\S|go.\\S";
//	//String pattern = "(team)(\\d+)";
//	Pattern r = Pattern.compile(pattern);
////	Matcher m = r.matcher(text.toLowerCase());
////
////
//////	
////	
////	while(m.find()){
////        System.out.println(m.group(1));
////
////		
////		
////        System.out.println("start(): "+m.start());
////        System.out.println("end(): "+m.end());	
////        System.out.println(text.substring(m.start(),m.end()));
////        }
//	
//	
//	//System.out.println(new java.io.File("").getAbsolutePath());
//	 HashMap<String, String> companyInfo;
//	 companyInfo = new HashMap<String,String>();
//	
//    FileInputStream fstream = new FileInputStream("cities.txt");
//    BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
//
//    String strLine;
//
//    //Read File Line By Linewhile ((line = br.readLine()) != null) {
//
//    while ((strLine = br.readLine()) != null)   {
//      // Print the content on the console
//    	companyInfo.put(strLine, "adsf");
//      //System.out.println (strLine);
//      String [] arr= strLine.split(" ");
//      
//     //S System.out.println(arr.length);
//      if(arr.length == 11){
//    	  String g = arr[0];
//    	 
//    	  if(g.length() > 3){
//    		  String twoLetterCode = g.substring(g.length()-2);
//    		  String threeLetterCode = arr[2];
//    		  String fullName =  g.substring(0,g.length()-2);
//    		  System.out.println(twoLetterCode);
//    		  System.out.println(threeLetterCode);
//    		  System.out.println("Printing Coutnry Name " + fullName + "\n");
//    		  companyInfo.put(twoLetterCode, fullName);
//    		  companyInfo.put(threeLetterCode, fullName);
//    		  
//    	  }
//      } else if(arr.length == 12){
//     	 //System.out.println(strLine);
//    	  String g = arr[1];
//    	  String [] a = g.split(" ");
//    	 
//    	  if(g.length() > 3){
//    		  String twoLetterCode = g.substring(g.length()-2);
//    		  String threeLetterCode = arr[3];
//    		  String fullName = arr[0] + " " + g.substring(0,g.length()-2);
//    		  System.out.println(twoLetterCode);
//    		  System.out.println(threeLetterCode);
//    		  System.out.println("Printing Coutnry Name " + fullName + "\n");
//
//    		  companyInfo.put(twoLetterCode, fullName);
//    		  companyInfo.put(threeLetterCode, fullName);
//
//
//    		  
//    	  }
//    	  System.out.println(g.substring(g.length()-2, g.length()));
//    	  System.out.println(g.substring(0, g.length()-1));
//    	  System.out.println(g.length());
//    	  System.out.println(g.charAt(g.length()-1));
    	  
//     	//System.out.println(arr[1]);
//     	//System.out.println(arr[0]);
//     	//String a = arr[0].split(" ");
//       } 
//      
//     // System.out.println(arr[1].substring(0, 1));
//    }
////
////    //Close the input stream
//   br.close();
//   
//	Iterator<String> it = companyInfo.keySet().iterator();
//
//   
//   while(it.hasNext()){
////		String val = it.next();
////		System.out.println(val);
////
////		 
////	}
//
////	
//}
//
//public static void getCodes() throws FileNotFoundException{
//	try{
//ArrayList <String> codes= new ArrayList<String>();
//String line = null;
//String file = "two.txt";
//String twoLetterAbbr;
//String threeLetterAbbr;
//String country;
//FileInputStream fis = new FileInputStream(file);
//
////Construct BufferedReader from InputStreamReader
////BufferedReader cityreader = new BufferedReader(new InputStreamReader(fis));
//
//BufferedReader cityreader = new BufferedReader(new FileReader(file));
//
////BufferedReader br = new BufferedReader(new FileReader(file));
//
//
//
//
//while ((line = cityreader.readLine()) != null) {
////if(cityreader.readLine() != null){
//codes.add( cityreader.readLine());
//System.out.println(cityreader.readLine());
////}	
//}
////	System.out.println(codes.get(3));
////	String [] parsedLine = codes.get(3).split("	");
////	System.out.println(parsedLine[1]);
////	
////	
////	for(int x =0; x< codes.size(); x++){
////		//System.out.println(codes.get(3));
////		String [] arsedLine = codes.get(x).split("	");
////		System.out.println(arsedLine[1]);
////	}
////	
//	
////for(String info: codes){
////	String [] arsedLine = info.split(" ");
////	System.out.println(arsedLine[0]);
////// twoLetterAbbr = codes.get(4).substring(0, 4);
////// threeLetterAbbr = codes.get(4).substring(3, 6);
////}
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//
//}
//
//}