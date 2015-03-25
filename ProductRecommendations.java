import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class ProductRecommendations{
  static private int SUPPORT = 100;
  static public HashMap<String, Integer> getFrequentItems(String inputfile) throws FileNotFoundException{
	  Scanner input = new Scanner(new File(inputfile));
    HashMap<String, Integer> itemsCnt = new HashMap<String, Integer>();
	while(input.hasNextLine()){
		String bucket[] = input.nextLine().split("\\s");
		for(String item: bucket){
			if(itemsCnt.containsKey(item))
				itemsCnt.put(item, itemsCnt.get(item)+1);
			else
			    itemsCnt.put(item, 1);
		}
	}
	input.close();
	return itemsCnt;
  }
  static public HashMap<Pair, Integer> getFrequentPairs(HashMap<String, Integer> frequentItems, String inputfile) throws FileNotFoundException{
	Scanner input = new Scanner(new File(inputfile));
	HashMap<Pair, Integer> frequentPairs = new HashMap<Pair, Integer>();
	while(input.hasNextLine()){
		String bucket[] = input.nextLine().split("\\s");
		if(bucket.length >= 2)
		{
		for( int i=0; i< bucket.length - 1; i++){
			for (int j = i+1; j< bucket.length; j++){
				 if(frequentItems.get(bucket[i]) >= SUPPORT &&
				    frequentItems.get(bucket[j]) >= SUPPORT &&
					!bucket[i].equals(bucket[j])){
				Pair p = new Pair(bucket[i], bucket[j]);
				if(frequentPairs.containsKey(p))
					frequentPairs.put(p, frequentPairs.get(p)+1);
				else
				    frequentPairs.put(p, 1);
				 }
			}
		}
		}
	}
	input.close();
	return frequentPairs;
  }
  static public HashMap<Triple, Integer> getFrequentTriples(HashMap<Pair, Integer> frequentPairs,HashMap<String, Integer> frequentItems, String inputfile) throws FileNotFoundException{
	Scanner input = new Scanner(new File(inputfile));
	HashMap<Triple, Integer> frequentTriples = new HashMap<Triple, Integer>();
	while(input.hasNextLine()){
		String bucket[] = input.nextLine().split("\\s");
		if(bucket.length >= 3){
			for (int i = 0; i< bucket.length - 2; i++){
				if (frequentItems.get(bucket[i]) >= SUPPORT)
				{
					for(int j = i+ 1; j< bucket.length - 1; j++){
						if(frequentItems.get(bucket[j]) >= SUPPORT){
							Pair p = new Pair(bucket[i], bucket[j]);
							if(frequentPairs.get(p)!=null && frequentPairs.get(p)>= SUPPORT){
								for( int k = j + 1; k < bucket.length; k++){
									if(frequentItems.get(bucket[k])>= SUPPORT){
									Pair p1 = new Pair(bucket[i], bucket[k]);
									Pair p2 = new Pair(bucket[j], bucket[k]);
									if(frequentPairs.get(p1)!=null && frequentPairs.get(p1)>= SUPPORT &&
									   frequentPairs.get(p2)!=null && frequentPairs.get(p2)>= SUPPORT){
										Triple t = new Triple(p, bucket[k]);
										if(frequentTriples.containsKey(t))
											frequentTriples.put(t,frequentTriples.get(t)+1);
										else
											frequentTriples.put(t,1);
									}
									}
								}
							}
						}
					}	
				}
			}
		}
	}
	input.close();
	return frequentTriples;
  }
  
  static public String top5confPair(HashMap<Pair, Integer> frequentPairs,HashMap<String, Integer> frequentItems){
	HashSet<Rule2> rules = new HashSet<Rule2>();
	for(Pair p: frequentPairs.keySet()){
		if(frequentPairs.get(p) >= SUPPORT){
			Rule2 r1 = new Rule2(p.min, p.max, ((float)frequentPairs.get(p))/frequentItems.get(p.min));
			Rule2 r2 = new Rule2(p.max, p.min, ((float)frequentPairs.get(p))/frequentItems.get(p.max));
			rules.add(r1);
			rules.add(r2);
		}
	}
	PriorityQueue<Rule2> pq = new PriorityQueue<Rule2>(5, new Comparator<Rule2>(){
		@Override
		public int compare(Rule2 r1, Rule2 r2) {
			if((r1.conf == r2.conf && r1.a.compareTo(r2.a) < 0) ||
					r1.conf > r2.conf)
					return 1;
				return -1;
		}});
		for(Rule2 r: rules){
			if(pq.size() < 5)
				pq.offer(r);
			else if(pq.peek().compareTo(r) < 0){
				pq.poll();
				pq.offer(r);
			}
		}
		StringBuilder sb = new StringBuilder();
		while(!pq.isEmpty()){
			sb.insert(0,pq.poll().toString());
			if(!pq.isEmpty()) 
				sb.insert(0,"\n");
		}
		System.out.print(sb.toString()+"\n");
		return sb.toString();
  }
  
  static public String top5confTriple(HashMap<Triple, Integer> frequentTriples, HashMap<Pair, Integer> frequentPairs){
	HashSet<Rule3> rules = new HashSet<Rule3>();
	for(Triple t: frequentTriples.keySet()){
		if(frequentTriples.get(t) >= SUPPORT){
			Pair p1 = new Pair(t.min, t.mid);
			Pair p2 = new Pair(t.min, t.max);
			Pair p3 = new Pair(t.mid, t.max);
			Rule3 r1 = new Rule3(p1, t.max, ((float)frequentTriples.get(t))/frequentPairs.get(p1));
			Rule3 r2 = new Rule3(p2, t.mid, ((float)frequentTriples.get(t))/frequentPairs.get(p2));
			Rule3 r3 = new Rule3(p3, t.min, ((float)frequentTriples.get(t))/frequentPairs.get(p3));
			rules.add(r1);
			rules.add(r2);
			rules.add(r3);
		}
	}
	PriorityQueue<Rule3> pq = new PriorityQueue<Rule3>(5, new Comparator<Rule3>(){
		public int compare(Rule3 r1, Rule3 r2){
			return r1.compareTo(r2);
		}});
		for(Rule3 r: rules){
			if(pq.size() < 5)
				pq.offer(r);
			else if(pq.peek().compareTo(r) < 0){
				pq.poll();
				pq.offer(r);
			}
		}
		StringBuilder sb = new StringBuilder();
		while(!pq.isEmpty()){
			sb.insert(0,pq.poll().toString());
			if(!pq.isEmpty()) 
				sb.insert(0,"\n");
		}
		System.out.print(sb.toString()+"\n");
		return sb.toString();
  }
  
  static public void main(String[] args) throws FileNotFoundException {
    HashMap<String, Integer> items = new HashMap<String, Integer>();
	HashMap<Pair, Integer> pairs = new HashMap<Pair, Integer>();
	HashMap<Triple, Integer> triples = new HashMap<Triple, Integer>();
	String inputfile = "browsing.txt";
	
	items = getFrequentItems(inputfile);
	pairs = getFrequentPairs(items, inputfile);
	triples = getFrequentTriples(pairs, items, inputfile);
	PrintWriter output1;
	PrintWriter output2;
	try {
		output1 = new PrintWriter("output1.txt");
		output1.write(top5confPair(pairs, items));
		output1.close();
		output2 = new PrintWriter("output2.txt");
		output2.write(top5confTriple(triples, pairs));
		output2.close();
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	}
  }
  
  public static class Rule2{
	public String a;
	public String b;
	public float conf;
	public Rule2( String a, String b, float conf){
		this.a = a;
		this.b = b;
		this.conf = conf;
	}
	public int hashCode(){
		final int prim = 67;
		return a.hashCode()*prim + b.hashCode();
	}
	public int compareTo(Rule2 r){
		if(conf == r.conf && a.compareTo(r.a) < 0 ||
		   conf > r.conf)
			return 1;
		return -1;
	}
	public String toString(){
		return "("+a+") =>"+b+" "+conf;
	}
 }
 
 public static class Rule3{
	public Pair p;
	public String c;
	public float conf;
	public Rule3(Pair p, String c, float conf){
		this.p = new Pair(p.min, p.max);
		this.conf = conf;
		this.c = c;
	}
	public int hashCode(){
		final int prim = 97;
		return p.hashCode() * prim + c.hashCode();
	}
	public int compareTo(Rule3 r){
		if((conf == r.conf
		    &&(p.min.compareTo(r.p.min) < 0 || 
		       (p.min.compareTo(r.p.min) == 0 && p.max.compareTo(r.p.max) < 0)))
		   || conf > r.conf)
			return 1;
		return -1;
	}
	public String toString(){
		return "("+p.min + "," +p.max+") => "+ c +" "+conf;
	}
 }
 
  public static class Pair{
    public String min;
	public String max;
	public Pair(String a, String b){
		min = a.compareTo(b)<0? a : b;
		max = a.compareTo(b)<0? b : a;
	}
	
	public boolean equals(Object o){
		if(o == null || this.getClass() != o.getClass())
			return false;
		Pair p = (Pair)o;
		if(min.equals(p.min) && max.equals(p.max))
			return true;
		return false;
	}
	public int hashCode(){
		final int prim = 89;
		return prim * min.hashCode() + max.hashCode();
	}
  }
  
  public static class Triple{
	public String min;
	public String max;
	public String mid;
	public Triple(Pair p, String c){
		String arr[] = {p.min, p.max, c};
		Arrays.sort(arr);
		min = new String(arr[0]);
		mid = new String(arr[1]);
		max = new String(arr[2]);
	}
	
	public boolean equals(Object o){
		if(o == null || this.getClass() != o.getClass())
			return false;
		Triple t = (Triple) o;
		if(min.equals(t.min) && max.equals(t.max) && mid.equals(t.mid))
			return true;
		return false;
	}
	public int hashCode(){
		final int prim1 = 137;
		final int prim2 = 83;
		return (min.hashCode()*prim1+mid.hashCode())*prim2+max.hashCode();
	}
  }
}
