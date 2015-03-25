package edu.stanford.cs246.peopleyoumightknow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.stanford.cs246.peopleyoumightknow.PeopleYouMightKnow;
import edu.stanford.cs246.peopleyoumightknow.PeopleYouMightKnow.Map;
import edu.stanford.cs246.peopleyoumightknow.PeopleYouMightKnow.Reduce;

public class PeopleYouMightKnow extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new PeopleYouMightKnow(), args);
	      
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "PeopleYouMightKnow");
	      job.setJarByClass(PeopleYouMightKnow.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      job.setMapOutputKeyClass(IntWritable.class);
	      job.setMapOutputValueClass(MutualFriend.class);
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	   }
	   
	   public static class MutualFriend implements Writable{
		   public int id;
		   public int cnt;
		   
		   public MutualFriend(){}
		   public void set(Integer userId, IntWritable friendCount){
			   id = userId.intValue();
			   cnt = friendCount.get(); 
		   }
		   
		   @Override
		   public void readFields(DataInput arg0) throws IOException {
			   id = arg0.readInt();
			   cnt = arg0.readInt();
		   }
		   @Override
		   public void write(DataOutput arg0) throws IOException {
			   arg0.writeInt(id);
			   arg0.writeInt(cnt);
		   }  
	   }
	   
	   public static class Map extends Mapper<LongWritable, Text, IntWritable, MutualFriend> {
	      private final static IntWritable ONE = new IntWritable(1);
	      private final static IntWritable ZERO = new IntWritable(0);
	      
	      private MutualFriend friendcnt = new MutualFriend();
	      private IntWritable user = new IntWritable();

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	 String line[] = value.toString().split("\t");
	    	 if(line.length == 2){
		    	 user.set(Integer.parseInt(line[0]));
		    	 String friendString[] = line[1].split(",");
		    	 if(friendString.length >= 1){
			    	 for(String friend: friendString){
			    		 friendcnt.set(Integer.parseInt(friend), ZERO);
			    		 context.write(user, friendcnt);
			    	 }
		    	 }
		    	 if(friendString.length > 1){
			    	 for(int i = 0; i< friendString.length - 1; i++){
			    		 for(int j = i + 1; j < friendString.length; j++){
			    			 user.set(Integer.parseInt(friendString[i]));
			    			 friendcnt.set(Integer.parseInt(friendString[j]), ONE);
			    			 context.write(user, friendcnt);
			    			 user.set(Integer.parseInt(friendString[j]));
			    			 friendcnt.set(Integer.parseInt(friendString[i]), ONE);
			    			 context.write(user, friendcnt);
			    		 }
			    	 }
		    	 }
	    	 }else{
	    		user.set(Integer.parseInt(line[0]));
	    		friendcnt.set(Integer.parseInt(line[0]), ZERO);
	    	 }
	      }
	   }

	   public static class Reduce extends Reducer<IntWritable, MutualFriend, Text, Text> {
		  private Text friendText = new Text();
		  private Text userText = new Text();
	      @Override
	      public void reduce(IntWritable key, Iterable<MutualFriend> values, Context context)
	              throws IOException, InterruptedException {
	    	 HashMap<Integer, Integer> map = new HashMap<Integer,Integer>(); 
	    	 for(MutualFriend value: values){
	    		 Integer count = map.get(value.id);
	    		 if (value.cnt == 0)
	    			 map.put(value.id, 0);
	    		 else if (count == null)
	    			 map.put(value.id, value.cnt);
	    		 else if (count != 0)
	    			 map.put(value.id, count+ value.cnt);
	    	 }
	    	 friendText.set(getFriendString(map));
	    	 userText.set(key.toString());
	    	 context.write(userText, friendText);
	      }
	      
	      private static String getFriendString(HashMap<Integer, Integer> map){
	    	  PriorityQueue<java.util.Map.Entry<Integer, Integer>> pq = new PriorityQueue<java.util.Map.Entry<Integer, Integer>>(10, new Comparator<java.util.Map.Entry<Integer, Integer>>(){
	    		  public int compare(java.util.Map.Entry<Integer, Integer> e1, java.util.Map.Entry<Integer, Integer> e2){
	    			  if(e1.getValue() == e2.getValue())
	    				  return e2.getKey().compareTo(e1.getKey());
	    			  return e1.getValue().compareTo(e2.getValue());
	    		  }
	    	  });
	    	  
	    	  for(java.util.Map.Entry<Integer, Integer> entry: map.entrySet()){
	    		  if(pq.size() < 10)
	    			  pq.offer(entry);
	    		  else if((pq.peek().getValue().equals(entry.getValue()) && pq.peek().getKey()>entry.getKey())||
	    				  (pq.peek().getValue() < entry.getValue())){
	    			  pq.poll(); 
	    		  	  pq.offer(entry);
	    		  }
	    	  }
	    	  
	    	  StringBuilder sb = new StringBuilder("");
	    	  while(!pq.isEmpty()){
	    		  java.util.Map.Entry<Integer, Integer> item = pq.poll();
	    		  if(item.getValue() == 0) continue;
	    		  sb.insert(0, item.getKey());
	    		  if(!pq.isEmpty()) sb.insert(0, ",");
	    	  }
	    	  return sb.toString();
	      } 
	   }
}
