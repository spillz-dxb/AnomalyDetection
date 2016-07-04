package ca.uwaterloo.iss4e;

import org.apache.hadoop.util.ProgramDriver;

import ca.uwaterloo.iss4e.clusteringbased.ClusteredSelfAnomaly;
import ca.uwaterloo.iss4e.datautils.DataPreparatorForClusteringBased;
import ca.uwaterloo.iss4e.datautils.DataPreparatorForPredictBased;
import ca.uwaterloo.iss4e.predictbased.GroupAnomaly;


/**
 * Created by xiuli on 2/29/16.
 */
public class Driver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("GroupAnomaly", GroupAnomaly.class,  "Group anomaly");
            pgd.addClass("SelfAnomaly", ca.uwaterloo.iss4e.predictbased.SelfAnomaly.class,  "Self anomaly");
            pgd.addClass("ClusteredSelfAnomaly", ClusteredSelfAnomaly.class,  "ClusteredSelfAnomaly");

            pgd.addClass("DataPreparatorForClusteringBased", DataPreparatorForClusteringBased.class,  "DataPreparatorForClusteringBased");
            pgd.addClass("DataPreparatorForPredictBased", DataPreparatorForPredictBased.class,  "DataPreparatorForPredictBased");

            exitCode = pgd.run(argv);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
