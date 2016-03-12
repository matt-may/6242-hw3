package edu.gatech.cse6242;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1 {
  public static class GraphMapper
   extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      // Split the line by tabs, into tokens.
      String line = value.toString();
      String[] tkns = line.split("\t");

      int src = Integer.parseInt(tkns[0]);
      int tgt = Integer.parseInt(tkns[1]);

      IntWritable wsrc = new IntWritable(src);
      Text wtgt = new Text(Integer.toString(tgt) + "|1");
      context.write(wsrc, wtgt);

      IntWritable wsrc2 = new IntWritable(tgt);
      Text wtgt2 = new Text(Integer.toString(src) + "|0");
      context.write(wsrc2, wtgt2);
    }
  }

  public static class EdgeWeightReducer
     extends Reducer<IntWritable,Text,IntWritable,Text> {
    // private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      // Go through all our values - if the current value is greater
      // than the max, set our max to the current value.
      List<String> inNodes = new ArrayList<String>();
      List<String> outNodes = new ArrayList<String>();

      for (Text val : values) {
        String v = val.toString();
        //String[] tkns = v.split("|");

        Text myText = new Text(v);

        context.write(key, myText);
        // if (Integer.parseInt(tkns[1]) == 0) {
        //   inNodes.add(tkns[1]);
        // }
        // else {
        //   outNodes.add(tkns[1]);
        // }
      }

      // for (String inNode : inNodes) {
      //   Text outVal = new Text(inNode);
      //
      //   context.write(key, outVal);
      // }

      // for (String outNode : outNodes) {
      //   Text outVal2 = new Text(outNode);
      //
      //   context.write(key, outVal2);
      // }

      //
      // IntWritable a = new IntWritable(1);
      // String str = "my string";
      // Text b = new Text(str);
      //
      // context.write(a, b);
      //
      // for (String inNode : inNodes) {
      //   IntWritable keyOut = new IntWritable(Integer.parseInt(inNode));
      //
      //   for (String outNode : outNodes) {
      //     System.out.println("outer loopssssss");
      //
      //     if (inNode != outNode) {
      //       System.out.println("inner looops ");
      //
      //       Text valOut = new Text(outNode);
      //
      //       context.write(keyOut, valOut);
      //     }
      //   }
      // }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", "\t");
    Job job = Job.getInstance(conf, "Task1");

    job.setJarByClass(Task1.class);
    job.setMapperClass(GraphMapper.class);
    job.setCombinerClass(EdgeWeightReducer.class);
    job.setReducerClass(EdgeWeightReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    // job.setOutputKeyClass(IntWritable.class);
    // job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
