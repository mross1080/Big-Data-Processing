import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


//We use this Counter in the Mapper to output how many 
enum CustomCounters {NUM_COMPANIES}

public class StockCompanyReplJoin {
   public static void runJob(String[] input, String output) throws Exception {

     Job job = Job.getInstance(new Configuration());
     Configuration conf = job.getConfiguration();
     job.setJarByClass(StockCompanyReplJoin.class);
     job.setMapperClass(StockCompanyReplJoinMapper.class);
     //note the job has no Reducer
     job.setMapOutputKeyClass(TextIntPair.class);
     job.setMapOutputValueClass(IntWritable.class);
     job.setInputFormatClass(StockInputFormat.class);
    
     job.setOutputFormatClass(SequenceFileOutputFormat.class);

     //this is a relative HDFS path (the file needs to be copied to this subfolder of your home)
     //this can be copied from the HDFS from /data/companylist or downloaded from QMPlus
     job.addCacheFile(new Path("cache/companylist.tsv").toUri());

     Path outputPath = new Path(output);
     FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
     FileOutputFormat.setOutputPath(job, outputPath);
     outputPath.getFileSystem(conf).delete(outputPath, true);
     job.waitForCompletion(true);
   }
  
   public static void main(String[] args) throws Exception {
     runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
   }
}