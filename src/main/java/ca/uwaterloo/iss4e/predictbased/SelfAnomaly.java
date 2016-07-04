package ca.uwaterloo.iss4e.predictbased;

/**
 * Created by xiuli on 6/27/15.
 */

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public final class SelfAnomaly {
    private static final Logger log = Logger.getLogger(SelfAnomaly.class);


    static class Split implements PairFunction<String, Tuple2<Integer, Integer>, Double[]> {
        @Override
        public Tuple2<Tuple2<Integer, Integer>, Double[]> call(String line) throws Exception {
            String[] fields = line.split("\\|");
            int meterID = Integer.valueOf(fields[0]); //MeterID
            String[] v = fields[1].split(" ");
            double readate = 1.0* Integer.valueOf(v[0].replace("-", ""));
            int hour = Integer.valueOf(v[1].split(":")[0]);
            double reading =  Double.parseDouble(fields[2]);
            double temperature =  Double.parseDouble(fields[3]);
            return new Tuple2<Tuple2<Integer, Integer>, Double[]>( // (meterID, hour)--> (reading, temperature, readdate)
                    new Tuple2<Integer, Integer>(meterID, hour), new Double[]{reading, temperature, readate});
        }
    }

    static class Train implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Double[]>>, Tuple2<Integer, Integer>, Double[]>{

        @Override
        public Tuple2<Tuple2<Integer, Integer>, Double[]> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Double[]>> tuple) throws Exception {
            try {
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();

            Iterator<Double[]> itr = tuple._2().iterator();
            ArrayList<Double[]> readTempDateList = new ArrayList<Double[]>();
            while (itr.hasNext()) {
                readTempDateList.add(itr.next());
            }
            Collections.sort(readTempDateList, new Comparator<Double[]>() {
                @Override
                public int compare(Double[] readTempDate1, Double[] readTempDate2) {
                    return readTempDate1[2]<readTempDate2[2]?-1:1;
                }
            });
            int nDay = readTempDateList.size();
            double[]Y = new double[nDay-3];
            double[][] X = new double[nDay-3][];
            for (int i=0; i<nDay-3; ++i){
                double[] x = new double[6];
                Double[] readingTemp0 = readTempDateList.get(i);
                Double[] readingTemp1 = readTempDateList.get(i+1);
                Double[] readingTemp2 = readTempDateList.get(i+2);
                Double[] readingTemp3 = readTempDateList.get(i+3);
                Y[i] = readingTemp3[0].doubleValue();
                x[0] = readingTemp2[0].doubleValue();
                x[1] = readingTemp1[0].doubleValue();
                x[2] = readingTemp0[0].doubleValue();

                double temp = readingTemp3[1].doubleValue();
                x[3] = temp > 20 ? temp - 20 : 0;
                x[4] = temp < 16 ? 16 - temp : 0;
                x[5] = temp < 5 ? 5 - temp : 0;
                X[i] = x;
            }
                regression.newSampleData(Y, X);
                double[] parameters = regression.estimateRegressionParameters();
                return new Tuple2<Tuple2<Integer, Integer>, Double[]>(tuple._1, ArrayUtils.toObject(parameters)); //(meterID, hour)-> parameters
            } catch (Exception e) {
                e.printStackTrace();
                return new Tuple2<Tuple2<Integer, Integer>, Double[]>(tuple._1, new Double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0});
            }
        }
    }

    public static void train(String inputDir, String parxModelDir, String normModelDir) {
        SparkConf conf = new SparkConf().setAppName("ClusteredSelfAnomaly-Training");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> trainingLines = sc.textFile(inputDir);

        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> paramRDD =
                trainingLines.mapToPair(new Split())
                .groupByKey()
                .mapToPair(new Train());

        paramRDD.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Double[]>, String, String>() {// Save PARX model to be used by the detection algorithm
            @Override
            public Tuple2<String, String> call(Tuple2<Tuple2<Integer, Integer>, Double[]> tuple) throws Exception {
                return new Tuple2<String, String>(tuple._1._1 + "|" + tuple._1._2 +"|"+ StringUtils.join(tuple._2, ","), null);
            }
        }).saveAsNewAPIHadoopFile(parxModelDir, Text.class, NullWritable.class, TextOutputFormat.class);

        trainingLines.mapToPair(new Split())
                 .cogroup(paramRDD)
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Iterable<Double[]>,Iterable<Double[]>>>, Tuple2<Integer,Integer>, Double>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Integer, Integer>, Double>> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Iterable<Double[]>, Iterable<Double[]>>> tuple) throws Exception {
                        Tuple2<Integer, Integer> meterIDHour = tuple._1;
                        int meterID = meterIDHour._1;
                        Iterator<Double[]> itr1 = tuple._2._1().iterator(); // Double[] = {reading, temperature, readdate}
                        Iterator<Double[]> itr2 = tuple._2._2().iterator(); // Double[] = {bata0, beta1, ..., ,beta6}
                        ArrayList<Double[]> readTempDateList = new ArrayList<Double[]>();
                        while (itr1.hasNext()) {
                            readTempDateList.add(itr1.next());
                        }
                        Collections.sort(readTempDateList, new Comparator<Double[]>() {
                            @Override
                            public int compare(Double[] readTempDate1, Double[] readTempDate2) {
                                return readTempDate1[2]<readTempDate2[2]?-1:1;
                            }
                        });
                        List<Tuple2<Tuple2<Integer, Integer>, Double>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>>();
                        if (itr2.hasNext()) {
                            Double[] beta = itr2.next();
                            int nDay = readTempDateList.size();
                            for (int i = 0; i < nDay - 3; ++i) {
                                double[] x = new double[6];
                                Double[] readingTempDate0 = readTempDateList.get(i);
                                Double[] readingTempDate1 = readTempDateList.get(i + 1);
                                Double[] readingTempDate2 = readTempDateList.get(i + 2);
                                Double[] readingTempDate3 = readTempDateList.get(i + 3);

                                x[0] = readingTempDate2[0].doubleValue();
                                x[1] = readingTempDate1[0].doubleValue();
                                x[2] = readingTempDate0[0].doubleValue();

                                double temp = readingTempDate3[1].doubleValue();
                                x[3] = temp > 20 ? temp - 20 : 0;
                                x[4] = temp < 16 ? 16 - temp : 0;
                                x[5] = temp < 5 ? 5 - temp : 0;

                                double actualReading = readingTempDate3[0].doubleValue();
                                double predictReading = beta[0];
                                for (int j = 1; j < beta.length; ++j) {
                                    predictReading += beta[j] * x[j - 1];
                                }
                                int date = (int) readingTempDate3[2].doubleValue();
                                ret.add(new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(meterID, date), (actualReading - predictReading) * (actualReading - predictReading)));
                            }
                        }
                        return ret;
                    }
                })
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Double>>, Integer, Double>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Double>> tuple) throws Exception {
                        int meterID = tuple._1._1;
                        Iterator<Double> itr = tuple._2.iterator();
                        double dailyVariance = 0.0;
                        while (itr.hasNext()) {
                            dailyVariance += itr.next();
                        }
                        return new Tuple2<Integer, Double>(meterID, Math.sqrt(dailyVariance));
                    }
                })
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<Integer, Iterable<Double>>, String, String>() {// Statistics
                    @Override
                    public Tuple2<String, String> call(Tuple2<Integer, Iterable<Double>> tuple) throws Exception {
                        Iterator<Double> itr = tuple._2.iterator();
                        List<Double> distances = new ArrayList<Double>();
                        double sum = 0.0;
                        while (itr.hasNext()) {
                            double dist = itr.next().doubleValue();
                            sum += dist;
                            distances.add(dist);
                        }
                        double mean = sum / distances.size();
                        double variance = 0.0;
                        for (double dist : distances) {
                            variance += (dist - mean) * (dist - mean);
                        }
                        double stdev = Math.sqrt(variance / distances.size());
                        return new Tuple2<String, String>(String.valueOf(tuple._1)+"|"+ mean + "," + stdev, null);
                    }
                }).saveAsNewAPIHadoopFile(normModelDir, Text.class, NullWritable.class, TextOutputFormat.class);

        sc.stop();
    }

    public static void detect(String parxModelDir, String normModelDir, String inputDir, final String outputDir, final double threshold) {
        SparkConf conf = new SparkConf().setAppName("ClusteredSelfAnomaly-Detect");
        final JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> paramLines = sc.textFile(parxModelDir);
        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> paramRDD = paramLines.mapToPair(new PairFunction<String, Tuple2<Integer, Integer>, Double[]>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double[]> call(String line) throws Exception {
                String[] fields = line.split("\\|");
                int meterID = Integer.valueOf(fields[0]);
                int hour = Integer.valueOf(fields[1]);
                String[] strArray = fields[2].split(",");
                Double[] parameters = new Double[strArray.length];
                for (int i = 0; i < strArray.length; ++i) {
                    parameters[i] = Double.valueOf(strArray[i]);
                }
                return new Tuple2<Tuple2<Integer, Integer>, Double[]>(new Tuple2<Integer, Integer>(meterID, hour), parameters);
            }
        });

        JavaRDD<String> distLines = sc.textFile(normModelDir);
        JavaPairRDD<Integer, Double[]> distRDD =
                distLines.mapToPair(new PairFunction<String, Integer, Double[]>() {
                    @Override
                    public Tuple2<Integer, Double[]> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        int meterID = Integer.valueOf(fields[0]);
                        String[] strArray = fields[1].split(",");
                        Double[] meanStdev = new Double[strArray.length];
                        for (int i = 0; i < strArray.length; ++i) {
                            meanStdev[i] = Double.valueOf(strArray[i]);
                        }
                        return new Tuple2<Integer, Double[]>(meterID, meanStdev);
                    }
                });

        JavaRDD<String> testLines = sc.textFile(inputDir);

        testLines.mapToPair(new PairFunction<String, Tuple2<Integer, Integer>, Double[]>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double[]> call(String line) throws Exception {
                String[] fields = line.split("\\|");
                int meterID = Integer.valueOf(fields[0]); //MeterID
                String[] v = fields[1].split(" ");
                double readate = 1.0* Integer.valueOf(v[0].replace("-", ""));
                int hour = Integer.valueOf(v[1].split(":")[0]);
                double reading =  Double.parseDouble(fields[2]);
                double temperature =  Double.parseDouble(fields[3]);
                return new Tuple2<Tuple2<Integer, Integer>, Double[]>( // (meterID, hour)--> (reading, temperature, readdate)
                        new Tuple2<Integer, Integer>(meterID, hour), new Double[]{reading, temperature, readate});
            }
        }) .cogroup(paramRDD)
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Iterable<Double[]>, Iterable<Double[]>>>, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Integer, Integer>, Double>> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Iterable<Double[]>, Iterable<Double[]>>> tuple) throws Exception {
                        Tuple2<Integer, Integer> meterIDHour = tuple._1;
                        int meterID = meterIDHour._1;
                        Iterator<Double[]> itr1 = tuple._2._1().iterator(); // Double[] = {reading, temperature, readdate}
                        Iterator<Double[]> itr2 = tuple._2._2().iterator(); // Double[] = {bata0, beta1, ..., ,beta6}
                        ArrayList<Double[]> readTempDateList = new ArrayList<Double[]>();
                        while (itr1.hasNext()) {
                            readTempDateList.add(itr1.next());
                        }
                        Collections.sort(readTempDateList, new Comparator<Double[]>() {
                            @Override
                            public int compare(Double[] readTempDate1, Double[] readTempDate2) {
                                return readTempDate1[2]<readTempDate2[2]?-1:1;
                            }
                        });

                        List<Tuple2<Tuple2<Integer, Integer>, Double>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>>();
                        if (itr2.hasNext() && readTempDateList.size() == 4) {
                            Double[] beta = itr2.next();
                            Double[] readingTempDate0 = readTempDateList.get(0);
                            Double[] readingTempDate1 = readTempDateList.get(1);
                            Double[] readingTempDate2 = readTempDateList.get(2);
                            Double[] readingTempDate3 = readTempDateList.get(3);

                            double[] x = new double[6];
                            x[0] = readingTempDate2[0].doubleValue();
                            x[1] = readingTempDate1[0].doubleValue();
                            x[2] = readingTempDate0[0].doubleValue();
                            double temp = readingTempDate3[1].doubleValue();
                            x[3] = temp > 20 ? temp - 20 : 0;
                            x[4] = temp < 16 ? 16 - temp : 0;
                            x[5] = temp < 5 ? 5 - temp : 0;
                            double predictReading = beta[0];
                            for (int j = 1; j < beta.length; ++j) {
                                predictReading += beta[j] * x[j - 1];
                            }
                            double actualReading = readingTempDate3[0].doubleValue();
                            int readdate = (int) readingTempDate3[2].doubleValue();
                            ret.add(new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(meterID, readdate), (actualReading - predictReading) * (actualReading - predictReading)));
                        }
                        return ret;
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double l2dist1, Double l2dist2) throws Exception {
                return l2dist1.doubleValue() + l2dist2.doubleValue();
            }
        }).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Double>, Integer, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Tuple2<Integer, Double>> call(Tuple2<Tuple2<Integer, Integer>, Double> tuple) throws Exception {
                return new Tuple2<Integer, Tuple2<Integer, Double>>(tuple._1._1, new Tuple2<Integer, Double>(tuple._1._2, tuple._2));
            }
        }).cogroup(distRDD)
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, Iterable<Double[]>>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double>>, Iterable<Double[]>>> tuple) throws Exception {
                        int meterID = tuple._1.intValue();
                        List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
                        Iterator<Double[]> itr2 = tuple._2._2.iterator();
                        if (itr2.hasNext()) {
                            Double[] meanStdev = itr2.next();
                            Iterator<Tuple2<Integer, Double>> itr1 = tuple._2._1.iterator();
                            while (itr1.hasNext()) {
                                Tuple2<Integer, Double> dateDist = itr1.next();
                                int readdate = dateDist._1.intValue();
                                double x = Math.sqrt(dateDist._2.doubleValue());
                                double propability = GroupAnomaly.phi(x, meanStdev[0], meanStdev[1]);
                               if (propability < threshold) {
                                    ret.add(new Tuple2<String, String>((meterID + "\t" + readdate), String.valueOf(propability)));
                               }
                            }
                        }
                        return ret;
                    }
                })
               // .coalesce(1, true)
                .saveAsNewAPIHadoopFile(outputDir, Text.class, Text.class, TextOutputFormat.class);
        sc.stop();
    }

    public static void main(String[] args) {
        String usage = "SelfAnomaly trainingFlag <parxModelDir> <normModelDir> <trainInputDir> <testInputDir> <outlierOutputDir> <threshold>";
        if (args.length != 7) {
            System.out.println(usage);
        }
        String parxModelDir = args[1];
        String normModelDir = args[2];
        String trainInputDir = args[3];
        String testInputDir = args[4];
        String outlierOutputDir = args[5];
        double threshold = Double.valueOf(args[6]);
        if (Integer.parseInt(args[0])==1) {
            SelfAnomaly.train(trainInputDir, parxModelDir, normModelDir);
        } else {
            SelfAnomaly.detect(parxModelDir, normModelDir, testInputDir, outlierOutputDir, threshold);
        }
    }
}