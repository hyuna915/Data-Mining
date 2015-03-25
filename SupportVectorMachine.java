import java.io.*;
import java.util.*;

import Jama.Matrix;


public class SupportVectorMachine{
	private static final int N = 6414;
	private static final int D = 122;
	
	private static final int BGD = 1;
	private static final int SGD = 2;
	private static final int MBGD = 3;
	
	private static final int BATCH = 20;
	private static int[] C = {1, 10, 50, 100, 200, 300, 400, 500};
	
	public static void main(String[] args) throws FileNotFoundException{
		Qf();
	}
	
	public static void Qf() throws FileNotFoundException{
		Matrix feaTrain = getFeature("features.train.txt");
		Matrix tarTrain = getTarget("target.train.txt");
		Matrix feaTest = getFeature("features.test.txt");
		Matrix tarTest = getTarget("target.test.txt");
		for(int i = 0; i< C.length; i++){
			Matrix w = getInitMatrix(1, D, 0.0);
			double b = SGD(feaTrain, tarTrain, 0.0001, 0.001, C[i], w);
			double result = getTestError(w, b, feaTest, tarTest);
			System.out.format(" c : %d, error: %f\n", C[i], result);
		}
	}
	public static void Qe() throws FileNotFoundException{
		Matrix feature = getFeature("features.txt");
		Matrix target = getTarget("target.txt");
		//BGD(feature, target, 0.0000003, 0.25, 100);
		//Matrix w = getInitMatrix(1, D, 0.0);
		//SGD(feature, target, 0.0001, 0.001, 100, w);
		MBGD(feature, target, 0.00001, 0.01, 100);
	}
	
	public static double SGD(Matrix feaO, Matrix tarO, double eta, double eps, int c, Matrix w){
		Matrix fea = getInitMatrix(feaO.getRowDimension(), D, 0.0);
		Matrix tar = getInitMatrix(1, feaO.getRowDimension(), 0.0);
		shuffle(feaO, tarO, fea, tar);
		double b = 0.0;
		int k = 0, mod = SGD, i = 1;
		double f = getF(w, b, c, fea, tar), newF = f;
		double delta = 0.0;
		double delta_percent = 0.0;
		long startTime = System.nanoTime();
		//System.out.format("SGD %d iteration cost: %f, delta: %f, i: %d, b: %f\n", k, newF, delta, i, b);
		do{
			for(int j = 0; j < D; j++){
				w.set(0, j, w.get(0, j)- eta * getGradientW(w, b, c, fea, tar, j, i, mod));
			}
			b = b - eta * getGradientB(w, b, c, fea, tar, i, mod);
			k++; 
			i = ((i+1)%fea.getRowDimension());
			newF = getF(w, b, c, fea, tar);
			delta_percent = getDeltaPercent(f, newF);
			delta = getDelta(delta_percent, delta);
			f = newF;
			//System.out.format("SGD %d iteration cost: %f, delta: %f, i: %d, b: %f\n", k, newF, delta, i, b);
		  }while(!isSatisfyCC(delta_percent,delta,eps, mod));
		System.out.format("iterations: %d", k);
		//System.out.format("SGD Time: %f ms \n", (System.nanoTime()-startTime)/1000000.0);
		return b;
	}
	
	public static double getGradientW(Matrix w, double b, double c, Matrix x, Matrix y, int j, int i, int mod){
		double gradW = w.get(0, j);
		if(mod == BGD){
			for(int k = 0; k < x.getRowDimension(); k++)
				if((y.get(0, k)*(b + getVectorProduct(w, getRow(x, k)))) < 1.0)
					gradW += c * (-1 * y.get(0, k)* x.get(k, j));
		}else if(mod == SGD){
			if(y.get(0, i)*(b + getVectorProduct(w, getRow(x, i))) < 1.0)
				gradW += (-1)*c* y.get(0, i) * x.get(i, j);
		}else{
			for(int k = i*BATCH+1; k < Math.min(N, (i+1)*BATCH); k++)
				if((y.get(0, k)*(b + getVectorProduct(w, getRow(x, k)))) < 1.0)
					gradW += c * (-1 * y.get(0, k)* x.get(k, j));
		}
		return gradW;
	}
	
	public static double getGradientB(Matrix w, double b, double c, Matrix x, Matrix y, int i, int mod){
		double gradB = 0.0;
		if(mod == BGD){
			for(int k = 0; k < x.getRowDimension(); k++)
				if(y.get(0, k)*(b + getVectorProduct(w, getRow(x, k))) < 1.0)
					gradB += c*(-1 * y.get(0, k));
		}else if(mod == SGD){
			if(y.get(0, i)*(b + getVectorProduct(w, getRow(x, i))) < 1.0){
				gradB += c*(-1 * y.get(0, i));
			}
		}else{
			for(int k = i*BATCH+1; k < Math.min(N, (i+1)*BATCH); k++)
				if((y.get(0, k)*(b + getVectorProduct(w, getRow(x, k)))) < 1.0)
					gradB += c*(-1 * y.get(0, k));
		}
		return gradB;
	}
	public static Matrix BGD(Matrix fea, Matrix tar, double eta, double eps, int c){
		Matrix w = getInitMatrix(1, D, 0.0);
		int mod = BGD, k = 0;
		double b = 0.0;
		double f = getF(w, b, c, fea, tar), newF = f;
		System.out.format("BGD %d iteration cost: %f \n", k, newF);
		double delta_percent = Double.MAX_VALUE;
		long startTime = System.nanoTime(); 
		while(!isSatisfyCC(delta_percent, -1, eps, mod)){
			for(int j = 0; j < D; j++){
				w.set(0, j, w.get(0, j)- eta * getGradientW(w, b, c, fea, tar, j, -1, mod));
			}
			b = b - eta * getGradientB(w, b, c, fea, tar, -1, mod);
			k++;
			newF = getF(w, b, c, fea, tar);
			delta_percent = getDeltaPercent(f, newF);
			f = newF;
			System.out.format("BGD %d iteration cost: %f b: %f delta_percent: %f\n", k, newF, b, delta_percent);
		}
		System.out.format("BGD Time: %f ms \n", (System.nanoTime()-startTime)/1000000.0);
		return w;
	}
	
	public static Matrix MBGD(Matrix feaO, Matrix tarO, double eta, double eps, int c){
		Matrix w = getInitMatrix(1, D, 0.0);
		Matrix fea = getInitMatrix(N, D, 0.0);
		Matrix tar = getInitMatrix(1, N, 0.0);
		shuffle(feaO, tarO, fea, tar);
		double b = 0.0;
		int k = 0, mod = MBGD, l = 0;
		double f = getF(w, b, c, fea, tar), newF = f;
		double delta = 0.0;
		double delta_percent = 0.0;
		System.out.format("MBGD %d iteration cost: %f, delta: %f, l: %d, b: %f\n", k, newF, delta, l, b);
		long startTime = System.nanoTime();
		do{
			for(int j = 0; j < D; j++){
				w.set(0, j, w.get(0, j)- eta * getGradientW(w, b, c, fea, tar, j, l, mod));
			}
			b = b - eta * getGradientB(w, b, c, fea, tar, l, mod);
			l = (l+1)%((N+BATCH-1)/BATCH);
			newF = getF(w, b, c, fea, tar);
			delta_percent = getDeltaPercent(f, newF);
			delta = getDelta(delta_percent, delta);
			k++; 
			f = newF;
			System.out.format("MBGD %d iteration cost: %f, delta: %f, l: %d, b: %f\n", k, newF, delta, l, b);
		}while(!isSatisfyCC(delta_percent,delta,eps, mod));
		System.out.format("MBGD Time: %f ms \n", (System.nanoTime()-startTime)/1000000.0);
		return w;
	}
	
	public static double getTestError(Matrix w,  double b, Matrix feaTest, Matrix tarTest){
		int miscnt = 0;
		for(int i = 0; i < feaTest.getRowDimension(); i++)
			if((tarTest.get(0, i)*(b + getVectorProduct(w, getRow(feaTest, i))))< 0.0)
				miscnt++;
		return ((double)miscnt)/feaTest.getRowDimension();
	}
	
	public static void shuffle(Matrix fea, Matrix tar, Matrix feaN, Matrix tarN){
		ArrayList<Integer> index = new ArrayList<Integer>();
		for(int i = 0; i < fea.getRowDimension(); i++)
			index.add(i);
		Collections.shuffle(index);
		for(int i = 0; i< fea.getRowDimension(); i++){
			tarN.set(0, i , tar.get(0, index.get(i)));
			for(int j = 0; j < D; j++)
				feaN.set(i,j, fea.get(index.get(i), j));
		}
	}
	
	public static boolean isSatisfyCC(double delta_percent, double delta, double eps, int mod){
			return mod == BGD? delta_percent < eps: delta < eps;
	}
	
	public static double getDeltaPercent(double f, double newF){
		return Math.abs(f-newF)*100/f;
	}
	
	public static double getDelta(double delta_percent, double delta){
		return 0.5*delta_percent + 0.5* delta;
	}
	
	public static double getF(Matrix w, double b, double c, Matrix x, Matrix y){
		double f = 0.0;
		for(int i = 0; i< D; i++){
			f+= 0.5*w.get(0,i)*w.get(0,i);
		}
		for(int i = 0; i < x.getRowDimension(); i++){
			f += c*Math.max(0.0, 1-y.get(0, i)*(b + getVectorProduct(w, getRow(x, i))));
		}
		return f;
	}
	
	public static Matrix getFeature(String featureFile) throws FileNotFoundException{
		int n = 0;
		Scanner in0 = new Scanner(new File(featureFile));
		while(in0.hasNextLine()){
			n++;
			in0.nextLine();
		}
		in0.close();
		Matrix m = new Matrix(n, D);
		Scanner in = new Scanner(new File(featureFile));
		for(int i = 0 ; in.hasNext(); i++){
			String[] s = in.nextLine().split(",");
			for(int j = 0; j < s.length; j++){
				m.set(i, j, Integer.parseInt(s[j]));
			}
		}
		in.close();
		return m;
	}

	public static Matrix getTarget(String targetFile) throws FileNotFoundException{
		int n = 0;
		Scanner in0 = new Scanner(new File(targetFile));
		while(in0.hasNextLine()){
			n++;
			in0.nextLine();
		}
		in0.close();
		Matrix m = new Matrix(1,n);
		Scanner in = new Scanner(new File(targetFile));
		for(int i = 0; in.hasNext(); i++){
			m.set(0, i, Integer.parseInt(in.nextLine()));
		}
		in.close();
		return m;
	}
	
	public static Matrix getInitMatrix(int row, int col, double val) {
		Matrix m = new Matrix(row, col);
		Random ran = new Random();
		for (int i = 0; i < row; i++)
			for (int j = 0; j < col; j++)
				m.set(i, j, val * ran.nextFloat());
		return m;
	}
	
	public static double getVectorProduct(Matrix p, Matrix q) {
		double product = 0.0;
		double[] pArr = p.getArray()[0];
		double[] qArr = q.getArray()[0];
		for (int i = 0; i < p.getColumnDimension(); i++)
			product += pArr[i] * qArr[i];
		return product;
	}
	
	public static Matrix getRow(Matrix m, int row){
		return m.getMatrix(row,row, 0, m.getColumnDimension()-1);
	}
}