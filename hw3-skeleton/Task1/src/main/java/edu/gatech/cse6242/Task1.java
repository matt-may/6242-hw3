package edu.gatech.cse6242;

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
   extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      // Split the line by tabs, into tokens.
      String line = value.toString();
      String[] tkns = line.split("\t");

      // Save our src node ID, and our weight.
      IntWritable src = new IntWritable(Integer.parseInt(tkns[0]));
      IntWritable weight = new IntWritable(Integer.parseInt(tkns[2]));

      // Write.
      context.write(src, weight);
    }
  }

  public static class EdgeWeightReducer
     extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      // Initialize our max to the first value.
      int max = values.iterator().next().get();

      // Go through all our values - if the current value is greater
      // than the max, set our max to the current value.
      for (IntWritable val : values) {
        int getVal = val.get();

        if (getVal > max) {
          max = getVal;
        }
      }

      // Set the result.
      result.set(max);

      // Write the result.
      context.write(key, result);
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
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
