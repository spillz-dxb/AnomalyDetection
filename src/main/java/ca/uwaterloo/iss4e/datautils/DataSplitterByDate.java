package ca.uwaterloo.iss4e.datautils;

import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Date;

/**
 * Created by xiuli on 3/4/16.
 */
public class DataSplitterByDate {

    public static void selectDataByDate(String inputDataDir, final String outputDataDir,  final String currentDateStr) {
        SparkConf conf = new SparkConf().setAppName("DataFomatter");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(inputDataDir);
        sc.broadcast(currentDateStr);

        lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                String[] fields = line.split("\\|");
                String[] dateTime = fields[1].split(" ");
                String readdateStr = dateTime[0];
                Date dateInRec = DateUtils.parseDate(readdateStr, new String[]{"yyyy-MM-dd"});
                Date currentDate = DateUtils.parseDate(currentDateStr, new String[]{"yyyyMMdd"});
                long diff=currentDate.getTime()-dateInRec.getTime();
                int noofdays=(int)(diff/(1000*24*60*60));

               return  (noofdays>=0 && noofdays<=3);
            }
        })
        .saveAsTextFile(outputDataDir);
        sc.stop();
    }


    public static void main(String[] args) {
        String usage = "DataSplitterByDate <inputDataDir> <outputDataDir> <currentDate>";
        if (args.length != 3) {
            System.out.println(usage);
        }
        String inputDataDir = args[0];
        String outputDataDir = args[1];
        String currentDate = args[2];
        DataSplitterByDate.selectDataByDate(inputDataDir, outputDataDir, currentDate);
    }
}
