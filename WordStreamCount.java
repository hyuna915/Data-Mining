import java.io.*;
import java.util.*;

public class WordStreamCount{
    private int [][] hashMatrix;
	private HashMap<Integer, Integer> wordCnt;
	private HashSet<Integer> S;
	private int[] params = new int[10];
	private int t;
	private int bucket;
	private int hash;
	public WordStreamCount(int bucket, int hash){
		this.bucket = bucket;
		this.hash = hash;
		wordCnt = new HashMap<Integer, Integer>();
		S = new HashSet<Integer>();
		hashMatrix = new int[hash][bucket];
	}
	
	public static void main(String[] args) throws IOException{
		WordStreamCount ws = new WordStreamCount(10000, 5);
		ws.getHashFunctions("hash_params.txt");
		ws.buildHashMatrix("words_stream.txt");
		//ws.getWordCount("counts.txt");
		Scanner in = new Scanner(new File("counts.txt"));
		ArrayList<Double> wordFreq = new ArrayList<Double>();
		ArrayList<Double> relatErr = new ArrayList<Double>();
		StringBuilder sb = new StringBuilder();
		for(int i = 0; in.hasNext(); i++){
			String[] token = in.nextLine().split("\\s");
			int id = Integer.parseInt(token[0]);
		    int cnt = Integer.parseInt(token[1]);
		    int predCnt = Integer.MAX_VALUE;
		    for(int j = 0; j <ws.hash; j++){
		    	int col = ws.hash(j, id);
		    	if(ws.hashMatrix[j][col] < predCnt);
		    		predCnt = ws.hashMatrix[j][col];
		    }
			wordFreq.add(Math.log10(cnt/((double)ws.t)));
			relatErr.add(Math.log10(Math.abs(predCnt-cnt)/((double)cnt)));
			sb.append(String.format("i: %d, id: %d, wf: %f, err: %f\n",i, id,Math.log10(cnt/((double)ws.t)),Math.log10(Math.abs(predCnt-cnt)/((double)cnt))));
		}
		FileWriter fr = new FileWriter("output_long.txt");
		fr.write(sb.toString());
		in.close();
		fr.close();
	}
	
	public int hash(int j, int x){
		int y = x%123457;
		int hash_val = (params[j*2]*y+params[j*2+1])%123457;
		return hash_val%bucket;
	}
	
	public void buildHashMatrix(String streamFile) throws FileNotFoundException{
		Scanner in = new Scanner(new File(streamFile));
		while(in.hasNext()){
			int id = Integer.parseInt(in.nextLine());
			S.add(id);
			for(int i = 0; i < hash; i++){
				int key = hash(i, id);
				hashMatrix[i][key]++;
			}
		}
		t = S.size();
		in.close();
	}
	
	public void getHashFunctions(String hashFile) throws FileNotFoundException{
		Scanner in = new Scanner(new File(hashFile));
		for(int i = 0; i< 5 && in.hasNext(); i++){
			String[] token = in.nextLine().split("\\s");
			params[i*2] = Integer.parseInt(token[0]);
			params[i*2+1] = Integer.parseInt(token[1]);
		}
	}
	public void getWordCount(String countFile) throws FileNotFoundException{
		Scanner in = new Scanner(new File(countFile));
		while(in.hasNext()){
			String[] token = in.nextLine().split("\\s");
			wordCnt.put(Integer.parseInt(token[0]),Integer.parseInt(token[1]));
		}
	}
	
	
	
	
}