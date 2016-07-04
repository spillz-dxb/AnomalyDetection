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
public class DataPreparatorForClusteringBased {
  private static final Logger log = Logger.getLogger(DataPreparatorForClusteringBased.class);

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
            if (StringUtils.isNotEmpty(arr[0]) && DataPreparatorForClusteringBased.isNumeric(arr[0])) {
              Date testDate = DateUtils.parseDate(arr[0], "yyyyMMdd");
              Date sDate = DateUtils.parseDate(startDate, "yyyyMMdd");
              Date eDate = DateUtils.parseDate(endDate, "yyyyMMdd");
              return isWithinRange(sDate, testDate, eDate);
            } else {
              return false;
            }
          }
        })
        .flatMapToPair(new PairFlatMapFunction<String, String, Tuple3<Integer, Double, Double>>() {
          public Iterable<Tuple2<String, Tuple3<Integer, Double, Double>>> call(String s) throws Exception {
            List<Tuple2<String, Tuple3<Integer, Double, Double>>> list = new ArrayList<Tuple2<String, Tuple3<Integer, Double, Double>>>();
            String[] fields = s.split("\t");
            if (fields.length > 2) {
              String meterID = fields[0];
              String timestamp = fields[1];
              String readingStr = fields[2];
              String tempStr = fields[7];
              if (StringUtils.isNotEmpty(readingStr) && DataPreparatorForClusteringBased.isNumeric(readingStr)) {
                Double reading = Double.parseDouble(readingStr);
                String[] arr = timestamp.replace("-", "").split(" ");
                String ID = meterID + arr[0].substring(2); //1000 120101
                Integer hour = Integer.parseInt(arr[1].split(":")[0]);
                Double temperature = Double.parseDouble(tempStr);
                list.add(new Tuple2<String, Tuple3<Integer, Double, Double>>(ID, new Tuple3<Integer, Double, Double>(hour, reading, temperature)));
              }
            }
            return list;
          }
        }).groupByKey()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple3<Integer, Double, Double>>>, String, Tuple3<String, Double[], Double[]>>() {
          @Override
          public Iterable<Tuple2<String, Tuple3<String, Double[], Double[]>>> call(Tuple2<String, Iterable<Tuple3<Integer, Double, Double>>> x) throws Exception {
            String ID = x._1();
            String meterID = ID.substring(0, ID.length() - 6);
            String YYMMDD = ID.substring(ID.length() - 6);

            Iterator<Tuple3<Integer, Double, Double>> itr = x._2().iterator();
            List<Tuple2<String, Tuple3<String, Double[], Double[]>>> list = new ArrayList<Tuple2<String, Tuple3<String, Double[], Double[]>>>();
            int cnt = 0;
            Double[] readings = new Double[24];
            Double[] temperatures = new Double[24];
            while (itr.hasNext()) {
              Tuple3<Integer, Double, Double> t = itr.next();
              int h = t._1();
              readings[h] = t._2();
              temperatures[h] = t._3();
              ++cnt;
            }
            //boolean isSparse = (zeros==23)&&(ones==1);
            if (cnt==24) {
              list.add(new Tuple2<String, Tuple3<String, Double[], Double[]>>(meterID, new Tuple3<String, Double[], Double[]>(YYMMDD, readings, temperatures)));
            }
            return list;
          }
        })
        .groupByKey()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple3<String, Double[], Double[]>>>, String, String>() {
          public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple3<String, Double[], Double[]>>> x) throws Exception {
            String meterID = x._1();
            Iterator<Tuple3<String, Double[], Double[]>> itr = x._2().iterator(); // (YYMMDD, readingArray)
            TreeMap<String, Tuple2<Double[], Double[]>> sortedMap = new TreeMap<String, Tuple2<Double[], Double[]>>();
            while (itr.hasNext()) {
              Tuple3<String, Double[], Double[]> t = itr.next();
              sortedMap.put(t._1(), new Tuple2<Double[], Double[]>(t._2(), t._3()));
            }

            Collection<String> YYMMDDs = sortedMap.keySet();
            String[] YYMMDDArray = YYMMDDs.toArray(new String[YYMMDDs.size()]);

            List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
            for (int i = 0; i < YYMMDDArray.length; ++i) {
              String YYMMDD = YYMMDDArray[i];
              Tuple2<Double[], Double[]> readingTemp = sortedMap.get(YYMMDD);
              String dailyReadingStr = StringUtils.join(readingTemp._1(), ",");
              String dailyTempStr = StringUtils.join(readingTemp._2(), ",");
              ret.add(new Tuple2<String, String>(meterID + YYMMDD + "|" + dailyReadingStr + "|" + dailyTempStr, null));
            }
            return ret;
          }
        })
        .sortByKey(true)
        .saveAsNewAPIHadoopFile(outputDir, NullWritable.class, Text.class, TextOutputFormat.class);
    sc.close();
  }

  public static class TupleComparator implements Comparator<String>, Serializable {
    @Override
    public int compare(String o1, String o2) {
      long ID1 = Long.parseLong(o1.split("\\|")[0]);
      long ID2 = Long.parseLong(o2.split("\\|")[0]);
      return ID1<ID2 ? 1 : 0;
    }
  }

  public static void main(String[] args) {
    String usage = "java ca.uwaterloo.iss4e.clustering.PrepareDataPoints srcInputDir pointOutputDir startDate endDate";
    if (args.length != 4) {
      log.error(usage);
    } else {
      DataPreparatorForClusteringBased.preparePoints(args[0], args[1], args[2], args[3]);

    }
  }
}
