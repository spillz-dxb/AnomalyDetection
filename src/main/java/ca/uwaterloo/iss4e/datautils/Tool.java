package ca.uwaterloo.iss4e.datautils;

//import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by xiuli on 7/9/15.
 */
public class Tool {
    public static String namenode = "hdfs://gho-admin:9000";

    static void touch(String filename) {
        try {
            Configuration conf = new Configuration();
            FileSystem.setDefaultUri(conf, namenode);
            FileSystem fileSys = FileSystem.get(conf);
            Path file = new Path(filename);
            FileStatus stat = fileSys.getFileStatus(file);
            long currentTime = System.currentTimeMillis();
            fileSys.setTimes(file, currentTime, currentTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    static void merge(String srcDir, String dstFile, boolean deleteSrc) {
        try {
            Configuration conf = new Configuration();
            FileSystem.setDefaultUri(conf, namenode);
            FileSystem fs = FileSystem.get(conf);
            Path srcPath = new Path(srcDir);
            Path dstFilePath = new Path(dstFile);
            FileUtil.copyMerge(fs, srcPath, fs, dstFilePath, deleteSrc, conf, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    static void merge(String srcDir, String dstFile) {
        merge(srcDir, dstFile, true);
    }

    public static void main(String[] args) {

        try {

            String cmd = args[0];
            if ("touch".equals(cmd)){
                Tool.touch(args[1]);
            }
            if ("merge".equals(cmd)){
                Tool.merge(args[1], args[2], Boolean.valueOf(args[3]));
            }
        } catch(Exception e){
            System.out.println("Usage: java ca.uwaterloo.iss4e.spark.abnormaldetect.Tool [touch|merge]");
        }
    }

}
