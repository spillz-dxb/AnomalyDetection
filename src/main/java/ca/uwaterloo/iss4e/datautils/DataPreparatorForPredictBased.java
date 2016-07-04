package ca.uwaterloo.iss4e.datautils;

/**
 * Created by xiuli on 6/22/16.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by xiuli on 6/1/16.
 */
public class DataPreparatorForPredictBased {
  private static final Logger log = Logger.getLogger(DataPreparatorForPredictBased.class);

  //private static final String fieldDelim = "\\t";

  public static boolean isNumeric(String str) {
    return str.matches("-?\\d+(.\\d+)?");
  }

  public static boolean isWithinRange(Date startDate, Date testDate, Date endDate) {
    return !(testDate.before(startDate) || testDate.after(endDate));
  }

  public static void preparePoints(final String inputDir, final String outputDir, final String startDate, final String endDate) {
    SparkConf conf = new SparkConf().setAppName("Liu: Prepare data points");

//    MeterID	datetime_est	      reading	 datetime_edt	        dayname	holiday	working_day	drybulb_temp
    //   19419	  2011-04-03 00:00:00	2.34	   2011-04-03 01:00:00	Sun	     0	       0	       3.5

    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.textFile(inputDir)
        .filter(new Function<String, Boolean>() {
          @Override
          public Boolean call(String s) throws Exception {
            String[] fields = s.split("\t");
            String timestamp = fields[1];
            String[] arr = timestamp.replace("-", "").split(" ");
            if (StringUtils.isNotEmpty(arr[0]) && DataPreparatorForPredictBased.isNumeric(arr[0])) {
              Date testDate = DateUtils.parseDate(arr[0], "yyyyMMdd");
              Date sDate = DateUtils.parseDate(startDate, "yyyyMMdd");
              Date eDate = DateUtils.parseDate(endDate, "yyyyMMdd");
              return isWithinRange(sDate, testDate, eDate);
            } else {
              return false;
            }
          }
        }).mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) throws Exception {
        String[] fields = s.split("\t");
        return new Tuple2<String, String>(fields[0]+"|"+fields[1]+"|"+fields[2]+"|"+fields[7], null);
      }
    }).saveAsNewAPIHadoopFile(outputDir, NullWritable.class, Text.class, TextOutputFormat.class);
    sc.close();
  }

  public static void main(String[] args) {
    String usage = "java ca.uwaterloo.iss4e.clustering.DataPreparatorForPredictBased srcInputDir pointOutputDir startDate endDate";
    if (args.length != 4) {
      log.error(usage);
    } else {
      DataPreparatorForPredictBased.preparePoints(args[0], args[1], args[2], args[3]);

    }
  }
}
