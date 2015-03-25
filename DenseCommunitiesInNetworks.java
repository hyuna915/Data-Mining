import java.util.*;
import java.io.*;

public class DenseCommunitiesInNetworks {
	private static final String FILE = "livejournal-undirected.txt";
	private static final String FILE_SMALL = "livejournal-undirected-small.txt";
	private static double[] EPS_ARR = { 0.1, 0.5, 1, 2 };

	public static void main(String[] args) throws IOException {
		Q3();
	}

	public static void Q1() throws FileNotFoundException {
		HashSet<Integer> vertex = getVertex(FILE);
		System.out.format("total : %d nodes", vertex.size());
		for (int i = 0; i < EPS_ARR.length; i++) {
			getDensestComunity(vertex, EPS_ARR[i], FILE);
		}
	}

	public static void Q2() throws FileNotFoundException {
		double eps = 0.05;
		HashSet<Integer> vertex = getVertex(FILE);
		getDensestComunity(vertex, eps, FILE);
	}

	public static void Q3() throws IOException {
		double eps = 0.05;
		HashSet<Integer> vertex = getVertex(FILE);
		findCommunities(vertex, 20, eps);
	}

	public static void findCommunities(HashSet<Integer> set, int k, double eps)
			throws IOException {
		HashSet<Integer> currSet = getVertex(FILE);
		for (int i = 1; i < k + 1; i++) {
			HashSet<Integer> densest = getDensestComunity(currSet, eps, FILE);
			currSet.removeAll(densest);
			System.out.format("Found %d th comunity! \n", k);
			if (currSet.size() == 0)
				break;
		}
	}

	public static HashSet<Integer> getDensestComunity(HashSet<Integer> vertex,
			double eps, String inFile) throws FileNotFoundException {
		HashSet<Integer> S = new HashSet<Integer>(vertex);
		HashSet<Integer> Sn = new HashSet<Integer>();
		double rhoMax = Double.MIN_VALUE;
		double e = getRho(getDense(inFile, S)) * S.size() * 0.5;
		int iter = 0;
		while (S.size() != 0) {
			iter++;
			HashMap<Integer, Integer> dense = getDense(inFile, S);
			HashSet<Integer> A = new HashSet<Integer>();
			System.out.format("i : %d ", iter);
			double rho = getRho(dense);
			for (Integer v : S) {
				if (dense.get(v) <= 2 * (1 + eps) * rho)
					A.add(v);
			}
			S.removeAll(A);
			HashMap<Integer, Integer> denseNew = getDense(inFile, S);
			double rhoNew = getRho(denseNew);
			if (rhoNew > rhoMax) {
				Sn = new HashSet<Integer>(S);
				rhoMax = rhoNew;
				e = rhoMax * Sn.size();
			}
		}
		System.out.format("size: %d, rho: %f, e: %f, totalIter: %d \n",
				Sn.size(), rhoMax, e, iter);
		return Sn;
	}

	public static double getRho(HashMap<Integer, Integer> dense) {
		int size = dense.keySet().size();
		if (size == 0)
			return Double.MIN_VALUE;
		int e = 0;
		for (int v : dense.keySet()) {
			e += dense.get(v);
		}
		System.out.format("rho: %f, size: %d e: %f\n", 0.5 * e / size, size,
				0.5 * e);
		return 0.5 * e / size;
	}

	public static HashMap<Integer, Integer> getDense(String inputFile,
			HashSet<Integer> vertex) throws FileNotFoundException {
		Scanner in = new Scanner(new File(inputFile));
		HashMap<Integer, Integer> dense = new HashMap<Integer, Integer>();
		for (Integer v : vertex) {
			dense.put(v, 0);
		}
		while (in.hasNextLine()) {
			StringTokenizer token = new StringTokenizer(in.nextLine());
			int src = Integer.parseInt(token.nextToken());
			int dst = Integer.parseInt(token.nextToken());
			if (dense.containsKey(src) && dense.containsKey(dst)) {
				dense.put(src, dense.get(src) + 1);
				dense.put(dst, dense.get(dst) + 1);
			}
		}
		in.close();
		return dense;
	}

	public static HashSet<Integer> getVertex(String inputFile)
			throws FileNotFoundException {
		Scanner in = new Scanner(new File(inputFile));
		HashSet<Integer> set = new HashSet<Integer>();
		while (in.hasNextLine()) {
			StringTokenizer token = new StringTokenizer(in.nextLine());
			set.add(Integer.parseInt(token.nextToken()));
			set.add(Integer.parseInt(token.nextToken()));
		}
		System.out.format("get %d vertex  ", set.size());
		return set;
	}

	public static HashSet<Integer> inputSet(int i) throws FileNotFoundException {
		File file = new File(String.format("%d.txt", i));
		Scanner in = new Scanner(file);
		HashSet<Integer> set = new HashSet<Integer>();
		while (in.hasNext()) {
			set.add(Integer.parseInt(in.nextLine()));
		}
		return set;
	}

	public static void outputSet(HashSet<Integer> set, int i)
			throws IOException {
		File file = new File(String.format("%d.txt", i));
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		StringBuffer sb = new StringBuffer();
		for (int v : set)
			sb.append(String.format("%d\n", v));
		bw.write(sb.toString());
		bw.close();
	}
}