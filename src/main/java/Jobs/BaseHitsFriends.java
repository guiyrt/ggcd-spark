package Jobs;

import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BaseHitsFriends {
    // These options must be declared in input, otherwise the job exists unsuccessfully
    private static final List<String> requiredOptions = Arrays.asList("titleBasicsTable",
            "titleRatingsTable",
            "titlePrincipalsTable",
            "nameBasicsTable"
    );

    /**
     * Executes RDDs operations that satisfy the second objective of this assignment
     * @param args Input arguments
     */
    public static void main(String[] args) {
        // Get input options
        Map<String, String> options = Common.Job.getInputOptions(args);
        List<String> missingOptions = Common.Job.missingOptions(options, requiredOptions);

        // Must contain required options
        if (missingOptions.size() > 0) {
            System.err.println("\u001B[31mMISSING OPTIONS:\u001B[0m Job not submitted, the following required options are missing: \u001B[33m"
                    + Common.Job.missingOptionsString(missingOptions) + "\u001B[0m");
            System.exit(1);
        }

        // Instantiate spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ShowRddOperations")
                .config("hive.metastore", "thrift://hive-metastore:9083")
                .enableHiveSupport()
                .getOrCreate();

    }
}
