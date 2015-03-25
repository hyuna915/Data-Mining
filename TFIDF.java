package edu.stanford.cs246.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDF extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new TFIDF(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "Job1");
      job.setJarByClass(TFIDF.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setMapperClass(Map1.class);
      job.setReducerClass(Reduce1.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path("shakespeare/"));
      FileOutputFormat.setOutputPath(job, new Path("output1"));

      job.waitForCompletion(true);
      
      Configuration conf2 = getConf();
      
      Job job2 = new Job(conf2, "Job2");
      job2.setJarByClass(TFIDF.class);

      job2.setMapperClass(Map2.class);
      job2.setReducerClass(Reduce2.class);

      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(DoubleWritable.class);

      job2.setInputFormatClass(TextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);

      TextInputFormat.addInputPath(job2, new Path("output1"));
      TextOutputFormat.setOutputPath(job2, new Path("output2"));

      job2.waitForCompletion(true);
      return 0;
   }
   
   public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();
      String inputFile = "";

      public void setup(Context context){
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        inputFile = fileSplit.getPath().getName();
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
          String[] words = value.toString().toLowerCase().split("\\W+");
         for (String token: words) {
        	   word.set(token+"&"+inputFile);
             context.write(word, ONE);
         }
         word.set("*&"+inputFile);
         context.write(word, new IntWritable(words.length));
      }
   }

   public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
      private IntWritable tf = new IntWritable(0);
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         tf.set(sum);
         context.write(key, tf);
      }
   }

   public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
      private Text word = new Text();
      private Text docnt = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
          String in1[] = value.toString().split("\\s+");
          String in2[] = in1[0].split("&");
          word.set(in2[0]);
          docnt.set(in2[1]+"&"+in1[1]);
          if(!in1[0].contains("*"))
            context.write(word, docnt);
         }
      }

      public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable tfidf = new DoubleWritable(0);
        private final static Double N = 38.0;
        private Text word = new Text();
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
         Iterator<Text> it = values.iterator();
         int n =0;
         ArrayList<String> words = new ArrayList<String>();
         ArrayList<Double> tfidfs = new ArrayList<Double>();
         while (it.hasNext()) {
            String[] in = it.next().toString().split("&");
            int cnt = Integer.parseInt(in[1]); 
            words.add(key+" in "+in[0]+" count:"+cnt);
            tfidfs.add((double) cnt);
            n++;
         }
         for(int i = 0; i < words.size(); i++){
        	 tfidf.set(tfidfs.get(i)*Math.log10(N/n));
             word.set(words.get(i)+"   n: "+ n);
             context.write(word, tfidf);
         }
      }
   }
    
}
