package ca.uwaterloo.iss4e.datautils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiuli on 8/12/15.
 */
public class DataReplicator {

    public static void expand(String inputDir, final String outputDir, int numOfOutputFiles, final int numOfCopies, final int step) {
        SparkConf conf = new SparkConf().setAppName("DataReplicator");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> csvFileRDD = sc.textFile(inputDir);
        csvFileRDD.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String line) throws Exception {
                List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
                ret.add(new Tuple2<String, String>(line, null));
                if (numOfCopies > 0) {
                    String[] fields = line.split("\\|");
                    int meterID = Integer.valueOf(fields[0]);
                    for (int i = 1; i <= numOfCopies; ++i) {
                        int newMeterID = meterID + step * i;
                        String newLine = StringUtils.replace(line, fields[0], String.valueOf(newMeterID));
                        ret.add(new Tuple2<String, String>(newLine, null));
                    }
                }
                return ret;
            }
        }).coalesce(numOfOutputFiles, true)
                .saveAsNewAPIHadoopFile(outputDir, Text.class, NullWritable.class, TextOutputFormat.class);
    }

    public static void main(String[] args) {
        String usage = "DataReplicator <inputDataDir>  <outputDir> <numOfOutputFiles> <numOfCopies> <step>";
        if (args.length != 5) {
            System.out.println(usage);
        }
        String inputDataDir = args[0];
        String outputDir = args[1];
        int numOfOutputFiles = Integer.valueOf(args[2]);
        int numOfCopies = Integer.valueOf(args[3]);
        int step = Integer.valueOf(args[4]);
        DataReplicator.expand(inputDataDir, outputDir, numOfOutputFiles, numOfCopies, step);
    }
}
