package ca.uwaterloo.iss4e.predictbased;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Iterator;

/**
 * Created by xiuli on 8/4/15.
 */
public class DataFomatter {
    public static void formatTSData(String tsDir, final String outputDir) {
        SparkConf conf = new SparkConf().setAppName("DataFomatter");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> paramLines = sc.textFile(tsDir);
        paramLines.mapToPair(new PairFunction<String, Tuple2<Integer, String>, Tuple3<Integer, Double, Double>>() {
            @Override
            public Tuple2<Tuple2<Integer, String>, Tuple3<Integer, Double, Double>> call(String line) throws Exception {
                String[] fields = line.split("\\|"); //2647198|2012-01-22 12:00:00|7.893900|3.821728
                int meterID = Integer.valueOf(fields[0]);
                String[] dateTime = fields[1].split(" ");
                String readdate = dateTime[0].replace("-", ".");
                int hour = Integer.valueOf(dateTime[1].split(":")[0]);
                double reading = Double.valueOf(fields[2]);
                double temperature = Double.valueOf(fields[3]);
                return new Tuple2<Tuple2<Integer, String>, Tuple3<Integer, Double, Double>>(new Tuple2<Integer, String>(meterID, readdate), new Tuple3<Integer, Double, Double>(hour, reading, temperature));
            }
        }).groupByKey()
                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, String>, Iterable<Tuple3<Integer, Double, Double>>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Tuple2<Integer, String>, Iterable<Tuple3<Integer, Double, Double>>> tuple) throws Exception {
                        int meterID = tuple._1._1;
                        String readdate = tuple._1._2;
                        Iterator<Tuple3<Integer, Double, Double>> itr = tuple._2.iterator();
                        Double[] readings = new Double[24];
                        Double[] temperatures = new Double[24];
                        while (itr.hasNext()) {
                            Tuple3<Integer, Double, Double> hourReadingTemp = itr.next();
                            readings[hourReadingTemp._1()] = hourReadingTemp._2();
                            temperatures[hourReadingTemp._1()] = hourReadingTemp._3();
                        }
                        StringBuffer buf = new StringBuffer();
                        buf.append(meterID).append("|").append(readdate).append("|").append(StringUtils.join(readings, ",")).append("|").append(StringUtils.join(temperatures, ","));
                        return new Tuple2<String, String>(buf.toString(), null);
                    }
                })
                .coalesce(1, true)
                .saveAsNewAPIHadoopFile(outputDir, Text.class, NullWritable.class, TextOutputFormat.class);
    }

    public static void formatModelData(String parxModelDir, String normModelDir, String outputDir) {
        SparkConf conf = new SparkConf().setAppName("AbnormalDetectionByPARX-Detect");
        final JavaSparkContext sc = new JavaSparkContext(conf);

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

        JavaRDD<String> paramLines = sc.textFile(parxModelDir);
        paramLines.mapToPair(new PairFunction<String, Integer, Tuple2<Integer, Double[]>>() {
            @Override
            public Tuple2<Integer, Tuple2<Integer, Double[]>> call(String line) throws Exception {
                String[] fields = line.split("\\|");
                int meterID = Integer.valueOf(fields[0]);
                int hour = Integer.valueOf(fields[1]);
                String[] strArray = fields[2].split(",");
                Double[] parameters = new Double[strArray.length];
                for (int i = 0; i < strArray.length; ++i) {
                    parameters[i] = Double.valueOf(strArray[i]);
                }
                return new Tuple2<Integer, Tuple2<Integer, Double[]>>(meterID, new Tuple2<Integer, Double[]>(hour, parameters));
            }
        }).cogroup(distRDD)
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double[]>>, Iterable<Double[]>>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Double[]>>, Iterable<Double[]>>> tuple) throws Exception {
                        int meterID = tuple._1;
                        String[] params = new String[24];
                        Iterator<Tuple2<Integer, Double[]>> itr1 = tuple._2._1.iterator();
                        while (itr1.hasNext()) {
                            Tuple2<Integer, Double[]> hourParams = itr1.next();
                            int hour = hourParams._1;
                            Double[] beta = hourParams._2;
                            params[hour] = StringUtils.join(beta, ",");
                        }

                        Iterator<Double[]> itr2 = tuple._2._2.iterator();
                        Double[] meanStd = new Double[2];
                        if (itr2.hasNext()) {
                            meanStd = itr2.next();
                        }
                        StringBuffer buf = new StringBuffer().append(meterID).append("|").append(StringUtils.join(params, ",")).append("|").append(StringUtils.join(meanStd, ","));
                        return new Tuple2<String, String>(buf.toString(), null);
                    }
                }).saveAsNewAPIHadoopFile(outputDir, Text.class, NullWritable.class, TextOutputFormat.class);
    }

    public static void main(String[] args) {
        String usage = "DataFomatter [data|model] <inputDataDir1>  <inputDataDir2> <outputDataDir>";
        if (args.length != 3) {
            System.out.println(usage);
        }
        String inputDataDir1 = args[1];
        String inputDataDir2 = args[2];
        String outputDataDir = args[3];
        if ("data".equals(args[0]))
            DataFomatter.formatTSData(inputDataDir1, outputDataDir);
        else
            DataFomatter.formatModelData(inputDataDir1, inputDataDir2, outputDataDir);
    }
}
