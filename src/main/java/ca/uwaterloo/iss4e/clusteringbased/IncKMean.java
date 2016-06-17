package ca.uwaterloo.iss4e.clusteringbased;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class IncKMean implements Serializable {
	double threshHold = .8;
	String name = "";

	ArrayList<Cluster> clusters = null;
	int numberOfCluster = 0;

	public IncKMean(String name, double th) {
		clusters = new ArrayList<Cluster>();
		threshHold = th;
		this.name = name;
	}

	public void doClustering(String[] point) {

		boolean isFit = false;
		for (int i = 0; i < clusters.size() && !isFit; i++) {
			if (clusters.get(i).fitInCluster(point)) {
				clusters.get(i).assignToCluster(point);
				isFit = true;
			}
		}

		if (!isFit) {
			Cluster cluster = new Cluster(numberOfCluster, threshHold);
			cluster.assignToEmptyCluster(point, 1);
			clusters.add(cluster);
			numberOfCluster++;
		}
	}
	
	public Cluster getNearestCluster(String[] point)
	{
		for (int i = 0; i < clusters.size(); i++) {
			Cluster nearestCluster = clusters.get(i);
			if (nearestCluster.fitInCluster(point))
				return nearestCluster;
		}
		
		return new Cluster(point, 1);
	}
	
	public int findClusterByCentroid(String[] center)
	{
		Vector<Double> centroid = ClusterHelper.getVector(center, false);
		for (int i = 0; i < clusters.size(); i++) {
			Cluster nearestCluster = clusters.get(i);
			if (nearestCluster.centroid.equals(centroid))
				return i;
		}
		
		return -1;
	}
	
	public boolean isFitInCluser(String[] point) {

		for (int i = 0; i < clusters.size(); i++) {
			if (clusters.get(i).fitInCluster(point))
				return true;
		}
		return false;
	}

	public String toString() {
		String clustersInfo = "";

		for (int i = 0; i < clusters.size(); i++)
			clustersInfo += clusters.get(i).toString() + "\n";
		return clustersInfo;
	}

	public void  updateCluster(String[] centroid, String[] sumOfCentroid, int sumOfPointInCluster)
	{
		String clusterInfo = "";
		int clusterIndex = findClusterByCentroid(centroid); 
		if (clusterIndex != -1)
		{
			Cluster cluster = clusters.get(clusterIndex);
			cluster.updateCluster(sumOfCentroid, sumOfPointInCluster);
			//clusterInfo = cluster.toString();
		}
		else
		{
			Cluster cluster = new Cluster(numberOfCluster, threshHold);
			cluster.assignToEmptyCluster(centroid, sumOfPointInCluster);
			clusters.add(cluster);
			numberOfCluster++;
			//clusterInfo = cluster.toString();
		}
		//return clusterInfo;
	}

	 public void loadCluster(String clusterInfo) {
		String[] clustersText = clusterInfo.split("\n");
		clusters = new ArrayList<Cluster>();
		
		int i = 0;
		for (String line : clustersText) {
			line = line.replaceAll("\\[", "").replaceAll("\\]", "");
			String[] info = line.split(";");
			System.out.println("Id: " + info[0]);
			int id = Integer.parseInt(info[0]);

			String centroid = info[1].replaceAll("\\(", "").replaceAll("\\)",
					"");
			System.out.println("centroid: " + centroid);

			System.out.println("Radius: " + info[2]);
			double radius = Double.parseDouble(info[2]);

			System.out.println("number of point: " + info[3]);
			int numberOfPoint = Integer.parseInt(info[3]);

			Cluster cluster = new Cluster(id, threshHold, centroid.split(","), radius,
					numberOfPoint);
			
			clusters.add(cluster);
		}

	}
}
