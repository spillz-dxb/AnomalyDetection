package ca.uwaterloo.iss4e.predictbased;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by xiuli on 7/19/15.
 */
public class GroupAnomaly {

    private static final Logger log = Logger.getLogger(GroupAnomaly.class);

    public static double phi(double x) {
        return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
    }

    // return phi(x, mu, signma) = Gaussian pdf with mean mu and stddev sigma
    public static double phi(double x, double mu, double sigma) {
        return phi((x - mu) / sigma) / sigma;
    }

    //        meterid |      readtime       | reading | temperature
//        ---------+---------------------+---------+-------------
//          19427 | 2012-06-30 00:00:00 |    1.55 |       22.60

    public static void train(String neighborsDir, String inputDir, String normModelDir) {
        SparkConf conf = new SparkConf().setAppName("GroupAnomaly-Training");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> neighborLines = sc.textFile(neighborsDir);
        JavaPairRDD<Integer, Integer> neigh2MeRDD = neighborLines
                .mapToPair(new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        return new Tuple2<Integer, Integer>(Integer.valueOf(fields[1]), Integer.valueOf(fields[0])); //MyMeterID-->neighbor
                    }
                });


        final JavaPairRDD<String, String> trainRDD = sc.textFile(inputDir)
                .mapToPair(new PairFunction<String, Integer, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public Tuple2<Integer, Tuple3<Integer, Integer, Double>> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        int meterID = Integer.valueOf(fields[0]);
                        String[] dateTime = fields[1].split(" ");
                        int readdate = Integer.valueOf(dateTime[0].replace("-", ""));
                        int hour = Integer.valueOf(dateTime[1].split(":")[0]);
                        double reading = Double.valueOf(fields[2]);
                        return new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(meterID, new Tuple3<Integer, Integer, Double>(readdate, hour, reading)); //(myMeterID, readdate)->(hour, Reading)
                    }
                }).cogroup(neigh2MeRDD)
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Tuple3<Integer, Integer, Double>>, Iterable<Integer>>>, Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>> call(Tuple2<Integer, Tuple2<Iterable<Tuple3<Integer, Integer, Double>>, Iterable<Integer>>> tuple) throws Exception {
                        int myMeterID = tuple._1;
                        Iterator<Tuple3<Integer, Integer, Double>> dateHourReadingItr = tuple._2._1.iterator();
                        Iterator<Integer> neigh2MeItr = tuple._2._2.iterator();
                        List<Integer> hostMeterIDs = new ArrayList<Integer>();
                        while (neigh2MeItr.hasNext()) {
                            hostMeterIDs.add(neigh2MeItr.next());
                        }
                        int nHour = 0;
                        double[] readings = new double[24];
                        List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>>();
                        while (dateHourReadingItr.hasNext()) {
                            ++nHour;
                            Tuple3<Integer, Integer, Double> dateHourReading = dateHourReadingItr.next();
                            int readdate = dateHourReading._1();
                            int hour = dateHourReading._2();
                            double reading = dateHourReading._3();
                            readings[hour] = reading;
                            if (nHour % 24 == 0) {
                                ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>(new Tuple2<Integer, Integer>(myMeterID, readdate), new Tuple2<Integer, double[]>(myMeterID, readings)));
                                for (int hostMeterID : hostMeterIDs) {
                                    ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>(new Tuple2<Integer, Integer>(hostMeterID, readdate), new Tuple2<Integer, double[]>(myMeterID, readings)));
                                }
                                readings = new double[24];
                            }
                        }
                        return ret;
                    }
                })
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, double[]>>>, Integer, Double>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, double[]>>> tuple) throws Exception {
                        int myMeterID = tuple._1._1.intValue();
                        double[] myReading = new double[24];
                        double[] myNeighReading = new double[24];
                        int neighCount = 0;
                        Iterator<Tuple2<Integer, double[]>> itr = tuple._2.iterator();
                        while (itr.hasNext()) {
                            Tuple2<Integer, double[]> meterIDReadings = itr.next(); //(neighborMeterID,  reading[24])
                            int neighMeterID = meterIDReadings._1().intValue();
                            double[] h24readings = meterIDReadings._2();
                            if (myMeterID == neighMeterID) {
                                myReading = h24readings;

                            } else {
                                boolean valid = true;
                                for (int i = 0; i < 24; ++i) {
                                    Double value = h24readings[i];
                                    if (value != null) {
                                        myNeighReading[i] += value.doubleValue();
                                    } else {
                                        for (int j = 0; j < i; ++j) {
                                            myNeighReading[j] -= h24readings[j];
                                        }
                                        valid = false;
                                        break;
                                    }
                                }
                                if (valid) ++neighCount;
                            }
                        }
                        double l2dist = 0;
                        for (int i = 0; i < 24; ++i) {
                            l2dist += Math.pow((myReading[i] - myNeighReading[i] / neighCount), 2);
                        }
                        return new Tuple2<Integer, Double>(myMeterID, Math.sqrt(l2dist));
                    }
                }).groupByKey()
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<Double>>, String, String>() {// Use the distances of all the days of a meter to compute the statistical model
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Integer, Iterable<Double>> tuple) throws Exception {
                        int meterID = tuple._1;

                        Iterator<Double> itr = tuple._2.iterator();
                        List<Double> distances = new ArrayList<Double>();
                        double sum = 0.0;
                        double variance = 0.0;
                        while (itr.hasNext()) {
                            double dist = itr.next().doubleValue();
                            distances.add(dist);
                            sum += dist;
                        }
                        int nDay = distances.size();
                        double mean = sum / nDay;
                        for (double dist : distances) {
                            variance += Math.pow((dist - mean), 2);
                        }
                        List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
                        if (mean > 0.0 && variance > 0)
                            ret.add(new Tuple2<String, String>(String.valueOf(meterID)+"|"+ mean + "," + Math.sqrt(variance / nDay), null));
                        return ret;
                    }
                });
        trainRDD.saveAsNewAPIHadoopFile(normModelDir, Text.class, NullWritable.class, TextOutputFormat.class);
        sc.stop();
    }


    public static void detect(String normModelDir, String neighborsDir, String inputDir, final String outputDir, final double epsilon) {
        SparkConf conf = new SparkConf().setAppName("GroupAnomaly-Detect");

        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> modelLines = sc.textFile(normModelDir);
        final JavaPairRDD<Integer, double[]> distRDD = modelLines
                .mapToPair(new PairFunction<String, Integer, double[]>() {
                    @Override
                    public Tuple2<Integer, double[]> call(String line) throws Exception {
                        String[] keyValue = line.split("\\|");
                        String[] meanStdev = keyValue[1].split(",");
                        return new Tuple2<Integer, double[]>(Integer.valueOf(keyValue[0]), new double[]{Double.valueOf(meanStdev[0]), Double.valueOf(meanStdev[1])});
                    }
                });

        JavaRDD<String> neighborLines = sc.textFile(neighborsDir);
        JavaPairRDD<Integer, Integer> neigh2MeRDD = neighborLines
                .mapToPair(new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        return new Tuple2<Integer, Integer>(Integer.valueOf(fields[1]), Integer.valueOf(fields[0])); //MyMeterID-->neighbor
                    }
                });


        JavaRDD<String> testLines = sc.textFile(inputDir);
        JavaPairRDD<String, String> results =
                testLines.mapToPair(new PairFunction<String, Integer, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public Tuple2<Integer, Tuple3<Integer, Integer, Double>> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        int meterID = Integer.valueOf(fields[0]);
                        String[] dateTime = fields[1].split(" ");
                        int readdate = Integer.valueOf(dateTime[0].replace("-", ""));
                        int hour = Integer.valueOf(dateTime[1].split(":")[0]);
                        double reading = Double.valueOf(fields[2]);
                        return new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(meterID, new Tuple3<Integer, Integer, Double>(readdate, hour, reading)); //(myMeterID, readdate)->(hour, Reading)
                    }
                }).cogroup(neigh2MeRDD)
                        .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Tuple3<Integer, Integer, Double>>, Iterable<Integer>>>, Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>() {
                            @Override
                            public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>> call(Tuple2<Integer, Tuple2<Iterable<Tuple3<Integer, Integer, Double>>, Iterable<Integer>>> tuple) throws Exception {
                                int myMeterID = tuple._1;
                                Iterator<Tuple3<Integer, Integer, Double>> dateHourReadingItr = tuple._2._1.iterator();
                                Iterator<Integer> neigh2MeItr = tuple._2._2.iterator();
                                List<Integer> hostMeterIDs = new ArrayList<Integer>();
                                while (neigh2MeItr.hasNext()) {
                                    hostMeterIDs.add(neigh2MeItr.next());
                                }
                                int nHour = 0;
                                double[] readings = new double[24];
                                List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>>();
                                while (dateHourReadingItr.hasNext()) {
                                    ++nHour;
                                    Tuple3<Integer, Integer, Double> dateHourReading = dateHourReadingItr.next();
                                    int readdate = dateHourReading._1();
                                    int hour = dateHourReading._2();
                                    double reading = dateHourReading._3();
                                    readings[hour] = reading;
                                    if (nHour % 24 == 0) {
                                        ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>(new Tuple2<Integer, Integer>(myMeterID, readdate), new Tuple2<Integer, double[]>(myMeterID, readings)));
                                        for (int hostMeterID : hostMeterIDs) {
                                            ret.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, double[]>>(new Tuple2<Integer, Integer>(hostMeterID, readdate), new Tuple2<Integer, double[]>(myMeterID, readings)));
                                        }
                                        readings = new double[24];
                                    }
                                }
                                return ret;
                            }
                        })
                        .groupByKey()
                        .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, double[]>>>, Integer, Tuple2<Integer, Double>>() {
                            @Override
                            public Tuple2<Integer, Tuple2<Integer, Double>> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, double[]>>> tuple) throws Exception {
                                int myMeterID = tuple._1._1;
                                int readdate = tuple._1._2;
                                double[] myReading = new double[24];
                                double[] myNeighReading = new double[24];
                                int neighCount = 0;
                                Iterator<Tuple2<Integer, double[]>> itr = tuple._2.iterator();
                                while (itr.hasNext()) {
                                    Tuple2<Integer, double[]> neighReadings = itr.next(); //(neighborMeterID, hour, Reading)
                                    int neighMeterID = neighReadings._1().intValue();
                                    double[] h24readings = neighReadings._2();
                                    if (myMeterID == neighMeterID) {
                                        myReading = h24readings;
                                    } else {
                                        boolean valid = true;
                                        for (int i = 0; i < 24; ++i) {
                                            Double value = h24readings[i];
                                            if (value != null) {
                                                myNeighReading[i] += value.doubleValue();
                                            } else {
                                                for (int j = 0; j < i; ++j) {
                                                    myNeighReading[j] -= h24readings[j];
                                                }
                                                valid = false;
                                                break;
                                            }
                                        }
                                        if (valid) ++neighCount;
                                    }
                                }
                                double dist = 0;
                                for (int i = 0; i < 24; ++i) {
                                    dist += Math.pow((myReading[i] - myNeighReading[i] / neighCount), 2);
                                }
                                dist = Math.sqrt(dist);
                                return new Tuple2<Integer, Tuple2<Integer, Double>>(myMeterID, new Tuple2<Integer, Double>(readdate, dist));
                            }
                        }).cogroup(distRDD)
                        .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, Iterable<double[]>>>, String, String>() {
                            @Override
                            public Iterable<Tuple2<String, String>> call(Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, Iterable<double[]>>> tuple) throws Exception {
                                List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
                                int myMeterID = tuple._1.intValue();
                                Iterator<double[]> itr1 = tuple._2._2.iterator();
                                if (itr1.hasNext()) {
                                    double[] meanStd = itr1.next();
                                    Iterator<Tuple2<Integer, Double>> itr2 = tuple._2._1.iterator();
                                    while (itr2.hasNext()) {
                                        Tuple2<Integer, Double> dateDist = itr2.next();
                                        int readdate = dateDist._1;
                                        double dist = dateDist._2.doubleValue();
                                        double probability = phi(dist, meanStd[0], meanStd[1]);
                                        if (probability < epsilon)
                                            ret.add(new Tuple2<String, String>(String.valueOf(myMeterID), readdate + "," + probability));
                                    }
                                }
                                return ret;
                            }
                        });
                        //.coalesce(1, true);
        results.saveAsNewAPIHadoopFile(outputDir, Text.class, Text.class, TextOutputFormat.class);

        sc.stop();
    }

    public static void main(String[] args) {
        String usage = "GroupAnomaly [train|test] <neighborsDir> <normModelDir> <inputDir> <outputDir> <epsilon>";
        if (args.length != 6) {
            System.out.println(usage);
        }
        String neighborsDir = args[1];
        String normModelDir = args[2];
        String inputDir = args[3];
        String outputDir = args[4];
        double epsilon = Double.valueOf(args[5]);
        if ("train".equals(args[0])) {
            GroupAnomaly.train(neighborsDir, inputDir, normModelDir);
        } else {
            GroupAnomaly.detect(normModelDir, neighborsDir, inputDir, outputDir, epsilon);
        }
    }
}
