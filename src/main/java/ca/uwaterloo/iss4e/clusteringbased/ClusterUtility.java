package ca.uwaterloo.iss4e.clusteringbased;
import java.util.Iterator;
import java.util.Vector;


public class ClusterUtility {

	public static Vector<Double> getVector(String[] points, boolean isNormalized) {

		Vector<Double> v = new Vector<Double>(points.length);

		for (String point : points) {
			double value = Double.parseDouble(point);
			if (Double.isNaN(value))
				value = 0.0;
			v.addElement(value);
		}

		return isNormalized == true? doNormalize(v) : v; // Always gives you normalized vector.
	}
	public static double getSquaredValueOfVector(Vector<Double> v) {

		double value = 0.0;
		Iterator<Double> it = v.iterator();
		while (it.hasNext()) {
			double valueInVector = (double) it.next();
			value += Math.pow(valueInVector, 2);

		}

		return Math.pow(value, .5);
	}

	public static Vector<Double> doNormalize(Vector<Double> v) {

		Vector<Double> normalizeVector = new Vector<Double>(v.size());
		double denom = getSquaredValueOfVector(v);

		if (denom == 0)
			return v;

		Iterator<Double> it = v.iterator();
		while (it.hasNext()) {
			double value = (double) it.next();
			normalizeVector.addElement(value / denom);
		}

		return normalizeVector;
	}

	public static double calculateEuclideanDistance(Vector<Double> v1,
			Vector<Double> v2) {

		double distance = 0.0;

		if (v1.size() == v2.size()) {
			Iterator<Double> it1 = v1.iterator();
			Iterator<Double> it2 = v2.iterator();

			while (it1.hasNext() && it2.hasNext()) {
				double value1 = (double) it1.next();
				double value2 = (double) it2.next();
				distance += Math.pow((value1 - value2), 2);
			}
		}

		return Math.pow(distance, .5);
	}

	public static double calculateCosineSimilarity(Vector<Double> v1, Vector<Double> v2) {

		double similarity = 0.0;

		if (v1.size() == v2.size()) {
			Iterator<Double> it1 = v1.iterator();
			Iterator<Double> it2 = v2.iterator();

			while (it1.hasNext() && it2.hasNext()) {
				double value1 = (double) it1.next();
				double value2 = (double) it2.next();
				similarity += (value1 * value2);
			}
		}

		return similarity;
	}
	
	public static double calculateSimilarity(Vector<Double> v1, Vector<Double> v2) {

		double distance = calculateEuclideanDistance(v1, v2);
		return 1 / (1 + distance);
	}

}
