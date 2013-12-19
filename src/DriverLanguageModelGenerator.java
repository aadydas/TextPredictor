import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class DriverLanguageModelGenerator {
  public static void main(String[] args) {

    JobClient client = new JobClient();
    // Configurations for Job set in this variable
    JobConf conf = new JobConf(DriverLanguageModelGenerator.class);

    // Name of the Job
    conf.setJobName("LangModelGenerator");

    // Data type of Output Key and Value
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // Setting the Mapper and Reducer Class
    conf.setMapperClass(mapper.class);
    conf.setReducerClass(reducer.class);

    // Formats of the Data Type of Input and output
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    // Specify input and output DIRECTORIES (not files)
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // Separator of the Input file
    conf.set("mapred.textoutputformat.separator", ",");
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 1) {
        System.err.println("wrong argument");
        System.exit(2);

    client.setConf(conf);
    try {
      // Running the job with Configurations set in the conf.
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}