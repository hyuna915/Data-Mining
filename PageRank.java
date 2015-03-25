import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.Random;

public class PageRank{
	static private int POWER_ITERATION = 40;
	static private double BETA= 0.8; 
	static int SIZE = 100;
	static private double[][] M;
	static private int[] K ={ 10,30, 50, 100}; 
	static private ArrayList<ArrayList<Integer>> graph;
	public static void main(String[] args) throws Exception{
		 
		String inputFile = "in.txt";
		graph = buildGraph(inputFile);
		M = buildMatrix(graph);
		
		long startTime = System.nanoTime();
		double[] pi = powerIteration(M, BETA, POWER_ITERATION);
		long endTime = System.nanoTime();
		System.out.format("PI time: %f ms \n", (endTime-startTime)/1000000.0);
		Arrays.sort(pi);
		//System.out.print("PI: ");
		//print(pi);
		for(int i = 1; i < 7; i+=2){
			for(int k = 0; k < K.length; k++){
				double errs = 0.0;
				double timer = 0.0;
				for(int j = 0; j < 100; j++){
					long st = System.nanoTime();
					double[] mc = MC(graph, BETA, i);
					long et = System.nanoTime();
					timer+= (et-st)/1000000.0;
					Arrays.sort(mc);
					errs += getError(pi, mc, K[k]);
				}
				System.out.format("MC R = %d, K = %d, time: %f ms \n",i,K[k], timer/100);
				System.out.format("R = %d K = %d, error = %f \n", i, K[k], errs/100.0);
			}
		}
	}
	
	public static void print(double[] arr){
		StringBuilder sb = new StringBuilder("( ");
		double sum = 0.0;
		for(int i = 0; i < arr.length; i++){
			sb.append(arr[i]+",");
			sum+=arr[i];
		}
		System.out.println(sum);
		System.out.println(sb.append(")").toString());
	}
	
	public static double[][] buildMatrix(ArrayList<ArrayList<Integer>> graph){
		M = new double[SIZE][SIZE];
		setMatrixAll(M, 0.0);
		for(int i = 0; i < SIZE; i++){
			double n = 1.0/graph.get(i+1).size();
			for(int j = 0; j < graph.get(i+1).size(); j++){
				M[j][i]+= n;
			}
		}
		return M;
	}

	public static ArrayList<ArrayList<Integer>> buildGraph(String inputFile) throws FileNotFoundException{
		Scanner in = new Scanner(new File(inputFile));
		ArrayList<ArrayList<Integer>> graph = new ArrayList<ArrayList<Integer>>();
		for(int i = 0; i< 101; i++)
			graph.add(new ArrayList<Integer>());
		while(in.hasNextLine()){
			String[] s = in.nextLine().split("\\s+");
			int src = Integer.parseInt(s[0]);
			int dst = Integer.parseInt(s[1]);
			graph.get(src).add(dst);
		}
		in.close();
		return graph;
	}
	
	public static double[] powerIteration(double[][] M, double beta, int iteration){
		double[] rnew = new double[SIZE];
		double[] rold = new double[SIZE];
		Arrays.fill(rold, 1.0/SIZE);
		Arrays.fill(rnew, 0.0);
		double tranpose = (1.0-beta)/SIZE;
		for (int i= 0; i< iteration; i++){
			rnew = multiplyMatrix(M, rold);
			rnew = multiplyVector(rnew, beta);
			rnew = addVector(rnew, tranpose);
			rold = rnew;
		}
		return rnew;
	}
	
	public static double[] MC(ArrayList<ArrayList<Integer>> graph, double beta, int iter){
		Random ran= new Random();
		double[] r = new double[SIZE];
		for(int i = 0; i < iter; i++)
			for(int j = 0; j < SIZE; j++){
				int pos = j;
				do{
					if(ran.nextFloat() <= beta){
						pos = getNext(graph.get(pos+1))-1;
						r[pos]+=1;
					}
					else
						break;
				} while(true);
			}
		
		return multiplyVector(r, (1-beta)/(SIZE*iter));
	}
	
	private static int getNext(ArrayList<Integer> list){
		Random ran = new Random();
		if(list == null || list.size() == 0)
			return -1;
		int index = ran.nextInt(list.size());
		return list.get(index);
	}


	public static double getError(double[] v1, double[] v2, int amount){
		double result = 0.0;
		for(int i =1; i <= amount; i++){
			result += Math.abs(v1[SIZE- i]-v2[SIZE-i]);
		}
		System.out.println(result);
		return result/amount;
	}
	
	public static void setMatrixAll(double[][] M, double val){
		for(int i = 0; i < M.length; i++)
			for(int j = 0; j< M[0].length; j ++)
				M[i][j] = val;
	}
	public static double[] multiplyMatrix(double[][] M, double[] vector){
		double[] result = new double[SIZE];
		Arrays.fill(result, 0.0);
		for(int i = 0; i < M.length; i++)
			for(int j = 0; j < M[0].length; j++)
				result[i] += M[i][j]*vector[j];
		return result;
	}
	public static double[] multiplyVector(double[] vector, double val){
		double[] result = new double[vector.length];
		for(int i = 0; i < vector.length; i++)
			result[i] = vector[i] * val;
		return result;
	}
	public static double[] addVector(double[] vector, double val){
		double[] result = new double[vector.length];
		for(int i = 0; i < vector.length; i++)
			result[i] = vector[i] + val;
		return result;
	}
}


