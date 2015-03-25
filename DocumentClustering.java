package edu.stanford.cs246.documentclustering;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DocumentClustering extends Configured implements Tool {
	final private static int ITERATION = 20;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new DocumentClustering(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		int i = 1;
		this.getConf().set("centroidDir", "c2.txt");
		for (i = 1; i <= ITERATION; i++) {
			Job job = new Job(getConf(), "DocumentClustering");
			job.setJarByClass(DocumentClustering.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path("data.txt"));
			FileOutputFormat.setOutputPath(job, new Path("centriod2_" + i
					+ "/"));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.waitForCompletion(true);
			getConf().set("centroidDir", "centriod2_" + i + "/part-r-00000");
		}
		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text docTxt = new Text();
		private Text cenTxt = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String inputfile = context.getConfiguration().get("centroidDir");
			ArrayList<Document> centriods = readCentriods(inputfile);
			Document doc = new Document(value.toString());
			Document center = centriods.get(0);
			double minDist = Double.MAX_VALUE;

			for (Document d : centriods) {
				double currDist = doc.eudDist(d);
				if (currDist < minDist) {
					center = d;
					minDist = currDist;
				}
			}
			docTxt.set(doc.toString());
			cenTxt.set(center.toString());
			context.write(cenTxt, docTxt);
		}

		private static ArrayList<Document> readCentriods(String inputfile)
				throws FileNotFoundException {
			Scanner input = new Scanner(new File(inputfile));
			ArrayList<Document> centriods = new ArrayList<Document>();
			while (input.hasNextLine()) {
				Document d = new Document(input.nextLine());
				centriods.add(d);
			}
			input.close();
			return centriods;
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
		private Text newCenTxt = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<Document> cluster = new ArrayList<Document>();
			for (Text d : values)
				cluster.add(new Document(d.toString()));
			Document newCentriod = Document.getNewCentriod(cluster);
			newCenTxt.set(newCentriod.toString());
			context.write(NullWritable.get(), newCenTxt);
		}
	}

	public static class Document {
		static final int DIM = 58;
		public double[] vec;

		public Document(String s) {
			vec = new double[DIM];
			String[] vecStrings = s.split(" ");
			for (int i = 0; i < DIM; i++)
				vec[i] = Double.parseDouble(vecStrings[i]);
		}

		public Document(double initVal) {
			vec = new double[DIM];
			for (int i = 0; i < DIM; i++)
				vec[i] = initVal;
		}

		public double eudDist(Document d) {
			double ssm = 0;
			for (int i = 0; i < DIM; i++) {
				ssm += (this.vec[i] - d.vec[i]) * (this.vec[i] - d.vec[i]);
			}
			return ssm;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			DecimalFormat df = new DecimalFormat("#0.0000");
			
			for (int i = 0; i < DIM; i++) {
				sb.append(df.format(this.vec[i]));
				sb.append(" ");
			}
			return sb.toString();
		}

		public void add(Document d) {
			for (int i = 0; i < DIM; i++)
				vec[i] += d.vec[i];
		}

		public void divide(double divisor) {
			for (int i = 0; i < DIM; i++)
				vec[i] /= divisor;
		}

		public static Document getNewCentriod(ArrayList<Document> cluster) {
			Document newCentriod = new Document(0.0);
			for (Document d : cluster) {
				newCentriod.add(d);
			}
			newCentriod.divide(cluster.size());
			return newCentriod;
		}

		public double getCost(ArrayList<Document> cluster) {
			double cost = 0.0;
			for (Document d : cluster)
				cost += eudDist(d);
			return cost;
		}
	}
}


package edu.stanford.cs246.documentclustering;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DocumentClusteringWithCost extends Configured implements Tool {
	private static final int ITERATION = 20;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new DocumentClusteringWithCost(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		int i = 1;
		this.getConf().set("centroidDir", "c2.txt");
		for (i = 1; i <= ITERATION; i++) {
			Job job = new Job(getConf(), "DocumentClusteringWithCost");
			job.setJarByClass(DocumentClustering.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path("data.txt"));
			FileOutputFormat.setOutputPath(job,
					new Path("cost2_" + i + "/"));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			getConf().set("centroidDir", "centriod2_" + i + "/part-r-00000");
			job.waitForCompletion(true);
		}
		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text docTxt = new Text();
		private Text cenTxt = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String inputfile = context.getConfiguration().get("centroidDir");
			ArrayList<Document> centriods = readCentriods(inputfile);
			Document doc = new Document(value.toString());
			Document center = centriods.get(0);
			double minDist = Double.MAX_VALUE;

			for (Document d : centriods) {
				double currDist = doc.eudDist(d);
				if (currDist < minDist) {
					center = d;
					minDist = currDist;
				}
			}
			docTxt.set(doc.toString());
			cenTxt.set(center.toString());
			context.write(cenTxt, docTxt);
		}

		private static ArrayList<Document> readCentriods(String inputfile)
				throws FileNotFoundException {
			Scanner input = new Scanner(new File(inputfile));
			ArrayList<Document> centriods = new ArrayList<Document>();
			while (input.hasNextLine()) {
				Document d = new Document(input.nextLine());
				centriods.add(d);
			}
			input.close();
			return centriods;
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
		private Text costTxt = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<Document> cluster = new ArrayList<Document>();
			for (Text d : values)
				cluster.add(new Document(d.toString()));
			Document newCentriod = Document.getNewCentriod(cluster);
			double cost = newCentriod.getCost(cluster);
			costTxt.set(Double.toString(cost));
			context.write(NullWritable.get(), costTxt);
		}
	}

	public static class Document {
		static final int DIM = 58;
		public double[] vec;

		public Document(String s) {
			vec = new double[DIM];
			String[] vecStrings = s.split(" ");
			for (int i = 0; i < DIM; i++)
				vec[i] = Double.parseDouble(vecStrings[i]);
		}

		public Document(double initVal) {
			vec = new double[DIM];
			for (int i = 0; i < DIM; i++)
				vec[i] = initVal;
		}

		public double eudDist(Document d) {
			double ssm = 0;
			for (int i = 0; i < DIM; i++) {
				ssm += (this.vec[i] - d.vec[i]) * (this.vec[i] - d.vec[i]);
			}
			return ssm;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < DIM; i++) {
				sb.append(this.vec[i]);
				sb.append(" ");
			}
			return sb.toString();
		}

		public void add(Document d) {
			for (int i = 0; i < DIM; i++)
				vec[i] += d.vec[i];
		}

		public void divide(double divisor) {
			for (int i = 0; i < DIM; i++)
				vec[i] /= divisor;
		}

		public static Document getNewCentriod(ArrayList<Document> cluster) {
			Document newCentriod = new Document(0.0);
			for (Document d : cluster) {
				newCentriod.add(d);
			}
			newCentriod.divide(cluster.size());
			return newCentriod;
		}

		public double getCost(ArrayList<Document> cluster) {
			double cost = 0.0;
			for (Document d : cluster)
				cost += eudDist(d);
			return cost;
		}
	}

}


package edu.stanford.cs246.documentclustering;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public class AutoTaggingDocuments{
	final static int TOP_VALUE = 5; 
	public static void main(String[] args) throws Exception{
		String centriodFile = "centriod1_20/part-r-00000";
		String tagsFile = "vocab.txt";
		Scanner sc = new Scanner(new File("data.txt"));
		ArrayList<Document> centriods = readCentriods(centriodFile);
		ArrayList<String> tags = readTags(tagsFile);
		Document doc = new Document(0);
		if(sc.nextLine()!=null)
			doc = new Document(sc.nextLine());
		sc.close();
        Document center = centriods.get(0);
        double minDist = Double.MAX_VALUE;

         for(Document d: centriods){
          double currDist = doc.eudDist(d);
          if( currDist < minDist){
            center = d;
            minDist = currDist;
          }
         }
        int[] features = center.getTop5();
        for(int i = TOP_VALUE-1; i >= 0; i--)
        	System.out.println(tags.get(features[i]));
	}

	private static ArrayList<String> readTags(String inputfile) throws FileNotFoundException{
		Scanner input = new Scanner (new File(inputfile));
		ArrayList<String> tagList = new ArrayList<String>();
		while(input.hasNextLine()){
			tagList.add(input.nextLine());
		}
		input.close();
		return tagList;
	}
	private static ArrayList<Document> readCentriods(String inputfile) throws FileNotFoundException{
        Scanner input = new Scanner (new File(inputfile));
        ArrayList<Document> centriods = new ArrayList<Document> ();
        while(input.hasNextLine()){
          Document d = new Document(input.nextLine());
          centriods.add(d);
        }
        input.close();
        return centriods;
      }

	public static class Document{
     static final int DIM = 58;
     public double [] vec;
     public Document(String s){
      vec = new double[DIM];
      String[]  vecStrings = s.split(" ");
      for(int i = 0; i< DIM; i++)
      vec[i] = Double.parseDouble(vecStrings[i]);
     }

     public Document(double initVal){
      vec = new double[DIM];
      for(int i = 0; i < DIM; i++)
          vec[i] = initVal;
     }
     
     public double eudDist(Document d){
       double ssm = 0;
       for(int i = 0; i < DIM; i++){
          ssm += (this.vec[i]- d.vec[i])*(this.vec[i]- d.vec[i]);
       }
       return Math.sqrt(ssm);
     }
     
     public String toString(){
       StringBuilder sb = new StringBuilder();
       for(int i = 0; i < DIM; i++){
          sb.append(this.vec[i]);
          sb.append(" ");
       }
       return sb.toString();
     }

     public void add(Document d){
        for(int i =0; i < DIM; i ++)
          vec[i] += d.vec[i];
     }

     public void divide(double divisor){
        for(int i = 0; i < DIM; i++)
          vec[i] /= divisor;
     }

     public static Document getNewCentriod(ArrayList<Document> cluster){
        Document newCentriod = new Document(0.0);
        for(Document d : cluster){
          newCentriod.add(d);
        }
        newCentriod.divide(cluster.size());
        return newCentriod;
     }

     public double getCost(ArrayList<Document> cluster){
      double cost = 0.0;
        for(Document d: cluster){
          cost += eudDist(d);
        }
        return cost;
     }

     public int[] getTop5(){
     	PriorityQueue<Item> pq = new PriorityQueue<Item>(TOP_VALUE, new Comparator<Item>(){
     		public int compare(Item i1, Item i2){
     			return i1.value < i2.value? -1: i1.value == i2.value? 0: 1;
     		}});
     	for(int i = 0; i < DIM; i++){
     		if(pq.size() < TOP_VALUE)
     			pq.offer(new Item(i, vec[i]));
     		else if (pq.peek().value < vec[i]){
     			pq.poll(); 
     			pq.offer(new Item(i, vec[i]));
     		}
     	}
     	int[] result = new int[TOP_VALUE];
     	int j = 0;
     	while(!pq.isEmpty()&& j<= TOP_VALUE)
     		result[j++] = pq.poll().index;
     	return result;
     }

     public class Item{
     	public int index;
     	public double value;
     	public Item(int index, double value){
     		this.index = index;
     		this.value = value; 
     	}
     }
   } 
}



