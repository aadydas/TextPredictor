import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class reducer extends MapReduceBase implements
    org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, DoubleWritable> {
  private static Map<String, String> map1;
  DoubleWritable probDouble;

  private static Map<String, Double> twoGram;
  private static Map<String, Double> threeGram;
  private static Map<String, Double> fourGram;
  private static Map<String, Double> fiveGram;
  private static String prevOneGram = "";
  private static String prevThreeGram = "";
  private static String prevFourGram = "";
  private static String prevTwoGram = "";
  private static Map finalMap;
  private static double twoGramCount;
  private static double threeGramCount;
  private static double fourGramCount;
  private static double oneGramCount;
  private static double fiveGramCount;

  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text, DoubleWritable> output, Reporter arg) throws IOException {

    while (values.hasNext()) {
      double probability = 1;
      String thisLine = key.toString().trim();

      double prob = 0.0;

      int length = thisLine.split(" ").length;
      double count = Double.parseDouble(values.next().toString());
      System.out.println("Count : " + count);

      if (length == 1) {
        oneGramCount = count;
        System.out.println("Count1 : " + count);
        if (twoGram != null) {
          output(output, sortByValue(twoGram));
          prevOneGram = "";
          twoGram = new HashMap<String, Double>();
          if (threeGram != null) {
            output(output, sortByValue(threeGram));
            prevTwoGram = "";
            threeGram = new HashMap<String, Double>();
          }
          if (fourGram != null) {
            output(output, sortByValue(fourGram));
            prevThreeGram = "";
            fourGram = new HashMap<String, Double>();
          }
          if (fiveGram != null) {
            output(output, sortByValue(fiveGram));
            prevFourGram = "";
            fiveGram = new HashMap<String, Double>();
          }

        }
      } else if (length == 2) {
        String prev = thisLine.substring(0, thisLine.lastIndexOf(" "));
        twoGramCount = count;
        System.out.println("Count2 : " + twoGramCount);
        if (threeGram != null) {
          output(output, sortByValue(threeGram));
          prevTwoGram = "";
          threeGram = new HashMap<String, Double>();
        }
        if (fourGram != null) {
          output(output, sortByValue(fourGram));
          prevThreeGram = "";
          fourGram = new HashMap<String, Double>();
        }
        if (fiveGram != null) {
          output(output, sortByValue(fiveGram));
          prevFourGram = "";
          fiveGram = new HashMap<String, Double>();
        }
        if ("".equals(prevOneGram)) {
          prevOneGram = prev;
          twoGram = new HashMap<String, Double>();
          prob = (twoGramCount / oneGramCount) * 100;
          twoGram.put(thisLine, prob);
        } else if (prev.equals(prevOneGram)) {
          prob = (twoGramCount / oneGramCount) * 100;
          System.out.println("Probability :" + prob);
          twoGram.put(thisLine, prob);
        } else {
          output(output, sortByValue(twoGram));
          // finalMap.putAll(sortByValue(twoGram));
          prevOneGram = prev;
          twoGram = new HashMap<String, Double>();
          prob = (twoGramCount / oneGramCount) * 100;
          twoGram.put(thisLine, prob);

        }

      } else if (length == 3) {
        String prev = thisLine.substring(0, thisLine.lastIndexOf(" "));
        threeGramCount = count;
        System.out.println("Count3 : " + threeGramCount);
        if (fourGram != null) {
          output(output, sortByValue(fourGram));
          prevThreeGram = "";
          fourGram = new HashMap<String, Double>();
        }
        if (fiveGram != null) {
          output(output, sortByValue(fiveGram));
          prevFourGram = "";
          fiveGram = new HashMap<String, Double>();
        }
        if ("".equals(prevTwoGram)) {
          prevTwoGram = prev;
          threeGram = new HashMap<String, Double>();
          prob = (threeGramCount / twoGramCount) * 100;
          threeGram.put(thisLine, prob);
        } else if (prev.equals(prevTwoGram)) {
          prob = (threeGramCount / twoGramCount) * 100;
          threeGram.put(thisLine, prob);
        } else {
          // finalMap.putAll(sortByValue(threeGram));
          output(output, sortByValue(threeGram));
          prevTwoGram = prev;
          threeGram = new HashMap<String, Double>();
          prob = (threeGramCount / twoGramCount) * 100;
          threeGram.put(thisLine, prob);

        }

      } else if (length == 4) {
        String prev = thisLine.substring(0, thisLine.lastIndexOf(" "));
        fourGramCount = count;
        System.out.println("Count4 : " + fourGramCount);
        if (fiveGram != null) {
          output(output, sortByValue(fiveGram));
          prevFourGram = "";
          fiveGram = new HashMap<String, Double>();
        }
        if ("".equals(prevThreeGram)) {
          prevThreeGram = prev;
          fourGram = new HashMap<String, Double>();
          prob = (fourGramCount / threeGramCount) * 100;
          fourGram.put(thisLine, prob);
        } else if (prev.equals(prevThreeGram)) {
          prob = (fourGramCount / threeGramCount) * 100;
          fourGram.put(thisLine, prob);
        } else {
          // finalMap.putAll(sortByValue(fourGram));
          output(output, sortByValue(fourGram));
          prevThreeGram = prev;
          fourGram = new HashMap<String, Double>();
          prob = (fourGramCount / threeGramCount) * 100;
          fourGram.put(thisLine, prob);

        }

      } else if (length == 5) {
        String prev = thisLine.substring(0, thisLine.lastIndexOf(" "));
        fiveGramCount = count;
        System.out.println("Count5 : " + fiveGramCount);
        if ("".equals(prevFourGram)) {
          prevFourGram = prev;
          fiveGram = new HashMap<String, Double>();
          prob = (fiveGramCount / fourGramCount) * 100;
          fiveGram.put(thisLine, prob);
        } else if (prev.equals(prevFourGram)) {
          prob = (fiveGramCount / fourGramCount) * 100;
          fiveGram.put(thisLine, prob);
        } else {
          // finalMap.putAll(sortByValue(fiveGram));
          output(output, sortByValue(fiveGram));
          prevFourGram = prev;
          fiveGram = new HashMap<String, Double>();
          prob = (fiveGramCount / fourGramCount) * 100;
          fiveGram.put(thisLine, prob);

        }

      }

    }

    for (Text val : values) {
      Put put = new Put(Bytes.toBytes(key.toString()));
      put.add(CF, COUNT, Bytes.toBytes(val.toString()));
      context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
    }
  }

  public static Map<String, Double> sortByValue(Map<String, Double> map) {
    List list = new LinkedList(map.entrySet());
    Collections.sort(list, new Comparator() {

      public int compare(Object o1, Object o2) {
        return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
      }
    });

    Map result = new LinkedHashMap();
    int count = 0;
    for (Iterator it = list.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      result.put(entry.getKey(), entry.getValue());
      count++;
      if (count >= 5) break;
    }
    return result;
  }

  public void output(OutputCollector<Text, DoubleWritable> output, Map<String, Double> map) {
    Iterator i = map.keySet().iterator();
    while (i.hasNext()) {
      Text key = new Text();
      String s = (String) i.next();
      s.substring(s.lastIndexOf(" ")+1);
      String finalWord =
          s.substring(0, s.lastIndexOf(" ")) + "," + s.substring(s.lastIndexOf(" ") + 1);
      key.set(finalWord);
      System.out.println("Key : " + s);
      System.out.println("Value : " + map.get(s));
      try {
        output.collect(key, new DoubleWritable(Math.round(map.get(s) * 100) / 100D));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}