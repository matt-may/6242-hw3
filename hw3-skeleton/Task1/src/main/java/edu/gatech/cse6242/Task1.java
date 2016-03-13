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
      Text wtgt = new Text(Integer.toString(tgt) + ":1");
      context.write(wsrc, wtgt);

      IntWritable wsrc2 = new IntWritable(tgt);
      Text wtgt2 = new Text(Integer.toString(src) + ":0");
      context.write(wsrc2, wtgt2);
    }
  }

  public static class EdgeWeightReducer
     extends Reducer<IntWritable,Text,IntWritable,Text> {

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      List<String> inNodes = new ArrayList<String>();
      List<String> outNodes = new ArrayList<String>();

      for (Text val : values) {
        String[] tkns = val.toString().split(":");

        String node = tkns[0];
        String direction = tkns[1];

        if (Integer.parseInt(direction) == 0) {
          inNodes.add(node);
        }
        else {
          outNodes.add(node);
        }
      }

      for (String inNode : inNodes) {
        IntWritable keyOut = new IntWritable(Integer.parseInt(inNode));

        for (String outNode : outNodes) {
          if (!inNode.equals(outNode)) {
            Text valOut = new Text(outNode);
            context.write(keyOut, valOut);
          }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", "\t");
    Job job = Job.getInstance(conf, "Task1");

    job.setJarByClass(Task1.class);
    job.setMapperClass(GraphMapper.class);
    //job.setCombinerClass(EdgeWeightReducer.class);
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
