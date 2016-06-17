package ca.uwaterloo.iss4e.clusteringbased;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

public class Cluster implements Serializable {
	int clusterId;
	Vector<Double> centroid = null;
	double radius = -999;

	int numberOfPoint = 0;
	double squaredValue = 0.0;
	String data;

	double threshHold = .80;

	public Cluster(int id, double th) {
		clusterId = id;
		threshHold = th;
	}

	// Single cluster
	public Cluster(String[] centroidPoint, int numberOfPoint) {
		this.centroid = ClusterHelper.getVector(centroidPoint, false);
		this.radius = 0;
		this.numberOfPoint = numberOfPoint;
	}

	public Cluster(int id, double th, String[] centroidPoint, double radius,
			int numberOfPoint) {
		clusterId = id;
		threshHold = th;
		this.centroid = ClusterHelper.getVector(centroidPoint, false);
		this.radius = radius;
		this.numberOfPoint = numberOfPoint;

	}

	public void assignToEmptyCluster(String[] points, int numberOfPointInCluster) {
		Vector<Double> v = ClusterHelper.getVector(points, false);
		centroid = v;
		radius = 0;
		numberOfPoint = numberOfPoint + numberOfPointInCluster ;
	}

	public void assignToCluster(String[] point) {
		Vector<Double> v = ClusterHelper.getVector(point, false);
		Vector<Double> updatedVector = new Vector<Double>(centroid.size());

		if (v.size() == centroid.size()) {

			if (!fitInCluster(v)) {
				System.out.println("No Cluster is fit for this point.");
				return;
			}

			Iterator<Double> it1 = v.iterator();
			Iterator<Double> it2 = centroid.iterator();
			numberOfPoint++;

			while (it1.hasNext() && it2.hasNext()) {
				double value1 = (double) it1.next();
				double value2 = (double) it2.next();
				double weightedAvg = (value1 + (numberOfPoint - 1) * value2)
						/ numberOfPoint;

				updatedVector.addElement(weightedAvg);

			}

			centroid = updatedVector;
			double distance = ClusterHelper.calculateEuclideanDistance(v,
					centroid);
			if (distance > radius)
				radius = distance;
		}
	}

	public void updateCluster(String[] point, int sumOfNumberOfPoints) {
		
		Vector<Double> v = ClusterHelper.getVector(point, false);
		Vector<Double> updatedVector = new Vector<Double>(centroid.size());

		if (v.size() == centroid.size()) {

			Iterator<Double> it1 = v.iterator();
			Iterator<Double> it2 = centroid.iterator();

			while (it1.hasNext() && it2.hasNext()) {
				double value1 = (double) it1.next();
				double value2 = (double) it2.next();
				double weightedAvg = (value1 + (numberOfPoint * value2))
						/ (numberOfPoint + sumOfNumberOfPoints);

				updatedVector.addElement(weightedAvg);
			}

			centroid = updatedVector;
			numberOfPoint = numberOfPoint + sumOfNumberOfPoints;

		}
	}

	public boolean fitInCluster(String[] point) {
		Vector<Double> v = ClusterHelper.getVector(point, false);

		return fitInCluster(v);
	}

	public boolean fitInCluster(Vector<Double> v) {
		// double similarity =
		// ClusterHelper.calculateCosineSimilarity(centroid, v);
		double similarity = ClusterHelper.calculateSimilarity(centroid, v);
		System.out.println("Similarity: " + similarity);
		return similarity >= threshHold;
	}

	public String toString() {
		String clusterInfoMessage = "";

		Iterator<Double> it = centroid.iterator();

		String center = "";
		while (it.hasNext()) {
			center += it.next().toString() + ",";
		}

		center = center.substring(0, center.length() - 1);

		clusterInfoMessage += "[" + clusterId + ";(" + center + ");" + radius
				+ ";" + numberOfPoint + "]";
		return clusterInfoMessage;
	}
}
