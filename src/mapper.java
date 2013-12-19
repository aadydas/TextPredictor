import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper implements
    org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable key = new IntWritable();
  static int count;

  public void map(LongWritable _key, Text value, OutputCollector<Text, IntWritable> output,
      Reporter reporter) throws IOException {
    String string = value.toString();
    String s[] = string.split("(\\s+)");
    String str2 = "";
    for (int i = 0; i < s.length - 1; i++) {
      str2 = str2 + s[i] + " ";
    }
    key.set(Integer.parseInt(s[s.length - 1]));
    output.collect(new Text(str2), key);
  }

  public void configure(JobConf arg0) {

  }

  public void close() throws IOException {
  }
}