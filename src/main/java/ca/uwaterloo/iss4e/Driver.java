package ca.uwaterloo.iss4e;

import org.apache.hadoop.util.ProgramDriver;

import ca.uwaterloo.iss4e.clusteringbased.IncClusteringAnomalyDetection;
import ca.uwaterloo.iss4e.predictbased.GroupAnomaly;
import ca.uwaterloo.iss4e.predictbased.SelfAnomaly;


/**
 * Created by xiuli on 2/29/16.
 */
public class Driver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("GroupAnomaly", GroupAnomaly.class,  "Group anomaly");
            pgd.addClass("SelfAnomaly", SelfAnomaly.class,  "Self anomaly");
            pgd.addClass("IncClusteringAnomalyDetection", IncClusteringAnomalyDetection.class,  "IncClusteringAnomalyDetection");

            exitCode = pgd.run(argv);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
