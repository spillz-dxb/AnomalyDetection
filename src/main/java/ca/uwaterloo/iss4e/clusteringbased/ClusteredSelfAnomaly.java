package ca.uwaterloo.iss4e.clusteringbased;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by xiuli on 5/30/16.
 */
public class ClusteredSelfAnomaly implements Serializable {

  private static final Logger log = Logger.getLogger(ClusteredSelfAnomaly.class);


  private static final String pointDelim = ",";
  private static final String keyDelim = "\\|";

  public static double phi(double x) {
    return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
  }

  // return phi(x, mu, signma) = Gaussian pdf with mean mu and stddev sigma
  public static double phi(double x, double mu, double sigma) {
    return phi((x - mu) / sigma) / sigma;
  }

  /**
   * @return (ID, hourlyDailyreadings), ID=meterID + YYMMDD,
   */

  protected static JavaPairRDD<String, double[]> loadPoints(JavaSparkContext sc, String pointInputDir) {
    return sc.textFile(pointInputDir)
        .mapToPair(new PairFunction<String, String, double[]>() {
          public Tuple2<String, double[]> call(String line) throws Exception {//1000 111201|2.1,1.2,3.1...,4.3
            String[] fields = line.split(keyDelim);
            String ID = fields[0];
            String[] points = fields[1].split(pointDelim);
            double[] pointArray = new double[points.length];
            for (int i = 0; i < points.length; ++i) {
              pointArray[i] = Double.parseDouble(points[i]);
            }
            return new Tuple2<String, double[]>(ID, pointArray);//(ID, hourlyreadingArray)
          }
        });
  }


  protected static JavaPairRDD<String, Tuple3<double[], Double, Double>> loadCentroids(JavaSparkContext sc, String centroidInputDir) {
    return sc.textFile(centroidInputDir)
        .mapToPair(new PairFunction<String, String, Tuple3<double[], Double, Double>>() {
          public Tuple2<String, Tuple3<double[], Double, Double>> call(String line) throws Exception {
            String[] fields = line.split(keyDelim);
            String meterID = fields[0];
            String[] centroidCollection = fields[1].split(pointDelim);
            String[] meanStddev = fields[2].split(pointDelim);
            double[] centroid = new double[centroidCollection.length];
            for (int i = 0; i < centroid.length; ++i) {
              centroid[i] = Double.parseDouble(centroidCollection[i]);
            }
            Double mean = Double.parseDouble(meanStddev[0]);
            Double stddev = Double.parseDouble(meanStddev[1]);
            return new Tuple2<String, Tuple3<double[], Double, Double>>(meterID, new Tuple3<double[], Double, Double>(centroid, mean, stddev));
          }
        });
  }

  /**
   * @param pointsRDD (ID, hourlyDailyreadings), ID=meterID + YYMMDD,
   */

  public static void train(JavaPairRDD<String, double[]> pointsRDD, final int maxK, final int maxIterations, final double coverage, String centroidOutputDir) {
    pointsRDD
        .mapToPair(new PairFunction<Tuple2<String, double[]>, String, double[]>() {
          @Override
          public Tuple2<String, double[]> call(Tuple2<String, double[]> x) throws Exception {
            String ID = x._1();
            String meterID = ID.substring(0, ID.length() - 6);
            return new Tuple2<String, double[]>(meterID, x._2());
          }
        })
        .groupByKey()
        .flatMapValues(new Function<Iterable<double[]>, Iterable<Tuple3<double[], Double, Double>>>() {// MeterID, all the points (hourlyReading of a day)
          @Override
          public Iterable<Tuple3<double[], Double, Double>> call(Iterable<double[]> x) throws Exception {
            List<DoublePoint> allPoints = new ArrayList<DoublePoint>();
            double[] overallMean = new double[24];
            int N = 0;
            Iterator<double[]> itr = x.iterator();
            while (itr.hasNext()) {
              double[] point = itr.next();
              for (int i = 0; i < 24; ++i) {
                overallMean[i] += point[i];
              }
              allPoints.add(new DoublePoint(point));
              ++N;
            }
            for (int i = 0; i < 24; ++i) {
              overallMean[i] = overallMean[i] / N;
            }

            List<Tuple3<double[], Double, Double>> ret = new ArrayList<Tuple3<double[], Double, Double>>();
            if (allPoints.size() > maxK) {
              double coverage = 0.0;
              int k = 1;
              while (coverage < coverage && k<maxK) {
                double SSW = 0.0;
                double SSB = 0.0;
                ret.clear();
                KMeansPlusPlusClusterer kmean = new KMeansPlusPlusClusterer(k, maxIterations);
                List<CentroidCluster<DoublePoint>> clusters = kmean.cluster(allPoints);
                EuclideanDistance euclideanDistance = new EuclideanDistance();
                for (CentroidCluster<DoublePoint> cluster : clusters) {
                  double[] centroid = cluster.getCenter().getPoint();
                  List<DoublePoint> pointsInCluster = cluster.getPoints();
                  double distBetween = euclideanDistance.compute(centroid, overallMean);
                  SSB +=  pointsInCluster.size()*(distBetween*distBetween);

                  SummaryStatistics stats = new SummaryStatistics();
                  for (DoublePoint point : pointsInCluster) {
                    double distWithin = euclideanDistance.compute(centroid, point.getPoint());
                    stats.addValue(distWithin);
                    SSW += distWithin * distWithin;
                  }
                  double mean = stats.getMean();
                  double stddev = stats.getStandardDeviation();
                  ret.add(new Tuple3<double[], Double, Double>(centroid, mean, stddev));
                }
                coverage = SSB/(SSB+SSW);
                k += 1;
              }
            }
            return ret;
          }
        }).mapToPair(new PairFunction<Tuple2<String, Tuple3<double[], Double, Double>>, String, String>() {
      @Override
      public Tuple2<String, String> call(Tuple2<String, Tuple3<double[], Double, Double>> x) throws Exception {
        StringBuffer buf = new StringBuffer();
        buf.append(x._1()).append("|");
        double[] centroid = x._2()._1();
        for (int i = 0; i < centroid.length; ++i) {
          buf.append(centroid[i]);
          if (i != centroid.length - 1) {
            buf.append(",");
          }
        }
        buf.append("|").append(x._2()._2()).append(",").append(x._2()._3());
        return new Tuple2<String, String>(buf.toString(), null);
      }
    }).saveAsNewAPIHadoopFile(centroidOutputDir, Text.class, NullWritable.class, TextOutputFormat.class);
  }


  public static void detect(JavaPairRDD<String, double[]> pointsRDD, JavaPairRDD<String, Tuple3<double[], Double, Double>> centroids, final double threshold, String outlierOutputDir) {
    pointsRDD.mapToPair(new PairFunction<Tuple2<String, double[]>, String, Tuple2<Integer, double[]>>() {
      @Override
      public Tuple2<String, Tuple2<Integer, double[]>> call(Tuple2<String, double[]> x) throws Exception {
        String ID = x._1();
        String meterID = ID.substring(0, ID.length() - 6);
        Integer date = Integer.parseInt("20" + ID.substring(ID.length() - 6));
        return new Tuple2<String, Tuple2<Integer, double[]>>(meterID, new Tuple2<Integer, double[]>(date, x._2()));
      }
    }).cogroup(centroids)
        .flatMapValues(new Function<Tuple2<Iterable<Tuple2<Integer, double[]>>, Iterable<Tuple3<double[], Double, Double>>>, Iterable<Tuple2<Integer, Double>>>() {
          @Override
          public Iterable<Tuple2<Integer, Double>> call(Tuple2<Iterable<Tuple2<Integer, double[]>>, Iterable<Tuple3<double[], Double, Double>>> x) throws Exception {
            Iterator<Tuple2<Integer, double[]>> itr1 = x._1().iterator();
            Iterator<Tuple3<double[], Double, Double>> itr2 = x._2().iterator();

            Map<double[], double[]> centroidStat = new HashMap<double[], double[]>();
            while (itr2.hasNext()) {
              Tuple3<double[], Double, Double> t = itr2.next();
              centroidStat.put(t._1(), new double[]{t._2(), t._3()});
            }

            List<Tuple2<Integer, Double>> list = new ArrayList<Tuple2<Integer, Double>>();
            if (centroidStat.size() > 0) {
              EuclideanDistance euclideanDistance = new EuclideanDistance();
              while (itr1.hasNext()) {
                Tuple2<Integer, double[]> datePoint = itr1.next();
                Integer date = datePoint._1();
                double[] point = datePoint._2();
                double minDist = Double.MAX_VALUE;
                double[] meanStddev = null;
                for (Map.Entry<double[], double[]> entry : centroidStat.entrySet()) {
                  double[] centroid = entry.getKey();
                  double dist = euclideanDistance.compute(centroid, point);
                  if (dist < minDist) {
                    minDist = dist;
                    meanStddev = entry.getValue();
                  }
                }
                double propability = phi(minDist, meanStddev[0], meanStddev[1]);
                if (propability < threshold) {
                  list.add(new Tuple2<Integer, Double>(date, propability));
                }
              }
            }
            return list;
          }
        }).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, String, String>() {
      @Override
      public Tuple2<String, String> call(Tuple2<String, Tuple2<Integer, Double>> x) throws Exception {

        return new Tuple2<String, String>(x._1() + "|" + x._2()._1() + "|" + x._2()._2(), null);
      }
    }).saveAsNewAPIHadoopFile(outlierOutputDir, Text.class, NullWritable.class, TextOutputFormat.class);
    ;
  }

  public static void run(int trainingFlag,
                         String trainingPointInputDir,
                         String testPointInputDir,
                         String centroidInputDir,
                         String outlierOutputDir,
                         int maxK,
                         int maxIteration,
                         double coverage,
                         double thredshold
  ) {

    SparkConf conf = new SparkConf().setAppName("Liu: Clustering-based anomaly detection");
    log.setLevel(Level.INFO);

    JavaSparkContext sc = new JavaSparkContext(conf);
    if (trainingFlag == 1) {
      JavaPairRDD<String, double[]> pointsRDD = ClusteredSelfAnomaly.loadPoints(sc, trainingPointInputDir);
      ClusteredSelfAnomaly.train(pointsRDD, maxK, maxIteration, coverage, centroidInputDir);
    } else {
      JavaPairRDD<String, double[]> pointsRDD = ClusteredSelfAnomaly.loadPoints(sc, testPointInputDir);
      JavaPairRDD<String, Tuple3<double[], Double, Double>> centroidRDD = ClusteredSelfAnomaly.loadCentroids(sc, centroidInputDir);
      ClusteredSelfAnomaly.detect(pointsRDD, centroidRDD, thredshold, outlierOutputDir);
    }
    sc.close();
  }

  public static void main(String[] args) {
    String usage = "java ca.uwaterloo.iss4e.clustering.ClusteredSelfAnomaly trainingFlag trainingPointInputDir testPointInputDir centroidInputDir outlierOutputDir maxK maxIteration coverage thredshold";
    if (args.length != 9) {
      log.error(usage);
    } else {
      ClusteredSelfAnomaly.run(
          Integer.parseInt(args[0]),
          args[1], //trainingPointInputDir
          args[2], //testPointInputDir
          args[3], //centroidInputDir
          args[4], //outlierOutputDir
          Integer.parseInt(args[5]), //maxK
          Integer.parseInt(args[6]), //maxIteration
          Double.parseDouble(args[7]), //maxIteration
          Double.parseDouble(args[8]) //thredshold
      );
    }
  }
}
