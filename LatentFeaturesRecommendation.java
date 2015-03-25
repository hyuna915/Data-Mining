import java.util.*;
import java.io.*;

import Jama.Matrix;

public class LatentFeaturesRecommendation {
	private static final int K = 20;
	private static double LAM = 0.2;
	private static double ITA = 0.02;
	private static final int ITERATION = 40;

	public static void main(String[] args) throws Exception {
		Qc();
	}

	public static void Qb() throws FileNotFoundException {
		String trainFile = "ratings.train.txt";
		int[] dims = getDimension(trainFile);
		Matrix initP = getInitMatrix(dims[0], K, Math.sqrt(5.0 / K));
		Matrix initQ = getInitMatrix(dims[1], K, Math.sqrt(5.0 / K));
		int k = 20, iteration = 40;
		double lam = 0.2, ita = 0.02;
		SGD(lam, ita, k, iteration, initP, initQ, trainFile, true);
	}

	public static void Qc() throws FileNotFoundException {
		String trainFile = "ratings.train.txt";
		String testFile = "ratings.val.txt";
		int[] dims = getDimension(trainFile);
		Matrix initP, initQ;
		int iteration = 40;
		double ita = 0.02;

		for (int k = 1; k < 11; k++) {
			initP = getInitMatrix(dims[0], k, Math.sqrt(5.0 / k));
			initQ = getInitMatrix(dims[1], k, Math.sqrt(5.0 / k));
			double errTr = SGD(0.2, ita, k, iteration, initP, initQ, trainFile,
					false);
			double errTe = getTestE(initP, initQ, testFile);
			System.out.format("k : %d, lam: 0.2, ita:0.02  Etr: %f, Ete: %f\n",
					k, errTr, errTe);
		}

		for (int k = 1; k < 11; k++) {
			initP = getInitMatrix(dims[0], k, Math.sqrt(5.0 / k));
			initQ = getInitMatrix(dims[1], k, Math.sqrt(5.0 / k));
			double errTr = SGD(0.0, ita, k, iteration, initP, initQ, trainFile,
					false);
			double errTe = getTestE(initP, initQ, testFile);
			System.out.format("k : %d, lam: 0, ita:0.02  Etr: %f, Ete: %f\n",
					k, errTr, errTe);
		}
	}

	public static double getTestE(Matrix p, Matrix q, String inFile)
			throws FileNotFoundException {
		Scanner in = new Scanner(new File(inFile));
		double errs = 0.0;
		while (in.hasNext()) {
			StringTokenizer token = new StringTokenizer(in.nextLine());
			int user = Integer.parseInt(token.nextToken());
			int movie = Integer.parseInt(token.nextToken());
			int rate = Integer.parseInt(token.nextToken());
			Matrix pu = p.getMatrix(user - 1, user - 1, 0,
					p.getColumnDimension() - 1);
			Matrix qi = q.getMatrix(movie - 1, movie - 1, 0,
					q.getColumnDimension() - 1);
			double err = (rate - getVectorProduct(pu, qi));
			errs += err * err;
		}
		return errs;
	}

	public static int[] getDimension(String trainFile)
			throws FileNotFoundException {
		int userMax = Integer.MIN_VALUE;
		int movieMax = Integer.MIN_VALUE;
		Scanner in = new Scanner(new File(trainFile));
		while (in.hasNext()) {
			StringTokenizer token = new StringTokenizer(in.nextLine());
			int user = Integer.parseInt(token.nextToken());
			int movie = Integer.parseInt(token.nextToken());
			if (user > userMax)
				userMax = user;
			if (movie > movieMax)
				movieMax = movie;
		}
		in.close();
		System.out.format("%d, %d\n", userMax, movieMax);
		return new int[] { userMax, movieMax };
	}

	public static Matrix getInitMatrix(int row, int col, double val) {
		Matrix m = new Matrix(row, col);
		Random ran = new Random();
		for (int i = 0; i < row; i++)
			for (int j = 0; j < col; j++)
				m.set(i, j, val * ran.nextFloat());
		return m;
	}

	public static double SGD(double lam, double ita, int k, int iter, Matrix p,
			Matrix q, String inFile, Boolean E) throws FileNotFoundException {
		double errs = 0.0;
		for (int i = 0; i < iter; i++) {
			Scanner in = new Scanner(new File(inFile));
			errs = 0.0;
			while (in.hasNext()) {
				StringTokenizer token = new StringTokenizer(in.nextLine());
				int user = Integer.parseInt(token.nextToken());
				int movie = Integer.parseInt(token.nextToken());
				int rate = Integer.parseInt(token.nextToken());
				// System.out.format("%d, %d, %d, %d \n", user, movie,
				// p.getColumnDimension()-1, q.getColumnDimension()-1);
				Matrix oldP = p.getMatrix(user - 1, user - 1, 0,
						p.getColumnDimension() - 1);
				Matrix oldQ = q.getMatrix(movie - 1, movie - 1, 0,
						q.getColumnDimension() - 1);
				double err = (rate - getVectorProduct(oldP, oldQ));
				errs += err * err;
				// System.out.println(errs+"\n");
				Matrix newP = oldP.plus((oldQ.times(err).minus(oldP.times(lam))
						.times(ita)));
				Matrix newQ = oldQ.plus((oldP.times(err).minus(oldQ.times(lam))
						.times(ita)));
				p.setMatrix(user - 1, user - 1, 0, p.getColumnDimension() - 1,
						newP);
				q.setMatrix(movie - 1, movie - 1, 0,
						q.getColumnDimension() - 1, newQ);
			}
			if (E) {
				errs += getPenalty(p, q, lam);
				// System.out.format("%f\n",errs);
			}
			if (!E)
				// System.out.format("Iteration : %d, E = %f\n", i,errs);
				in.close();
		}
		return errs;
	}

	public static double getPenalty(Matrix p, Matrix q, double lam) {
		double result = 0.0;
		for (int i = 0; i < p.getRowDimension(); i++)
			result += Math.pow(p.getMatrix(i, i, 0, p.getColumnDimension() - 1)
					.norm2(), 2.0);
		for (int i = 0; i < q.getRowDimension(); i++)
			result += Math.pow(q.getMatrix(i, i, 0, q.getColumnDimension() - 1)
					.norm2(), 2.0);
		return result * lam;
	}

	public static double getVectorProduct(Matrix p, Matrix q) {
		double product = 0.0;
		double[] pArr = p.getArray()[0];
		double[] qArr = q.getArray()[0];
		for (int i = 0; i < p.getColumnDimension(); i++)
			product += pArr[i] * qArr[i];
		return product;
	}

}