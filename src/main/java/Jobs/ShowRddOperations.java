package Jobs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class ShowRddOperations {
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

        // This first RDD will be used twice, so we can save the fetch and filter time by caching
        // All Datasets will select only the necessary columns
        JavaRDD<Row> titleBasicsWithStartYear = sparkSession.table(options.get("titleBasicsTable"))
                .select("tconst", "primaryTitle", "startYear", "genres")
                .toJavaRDD()
                .filter(row -> !row.isNullAt(2))
                .cache();
        JavaRDD<Row> titleRatings = sparkSession
                .table("titleRatings")
                .select("tconst", "averageRating")
                .toJavaRDD();
        JavaRDD<Row> titlePrincipals = sparkSession
                .table("titlePrincipals")
                .select("tconst", "nconst", "category")
                .toJavaRDD();
        JavaRDD<Row> nameBasics = sparkSession
                .table("nameBasics")
                .select("nconst", "primaryName")
                .toJavaRDD();

        // Top Genres
        System.out.println("\nTop Genres:");
        Actions.TopGenres
            .getTopGenresRDD(titleBasicsWithStartYear)
            .collect()
            .forEach(decade -> System.out.format("\tDecade: %d, Genre: %s\n", decade._1, decade._2));

        // Season Hits
        System.out.println("\nSeason Hits:");
        Actions.SeasonHits
            .getSeasonHitsRDD(titleRatings, titleBasicsWithStartYear)
            .collect()
            .forEach(season -> System.out.format("\tYear: %d, Title: %s, Rating: %s\n", season._1, season._2._1, season._2._2.toString()));

        // Top 10 Actors
        System.out.println("\nTop 10 Actors:");
        Actions.TopActors
            .getTopActorsRDD(titlePrincipals, nameBasics)
            .take(10)
            .forEach(actor -> System.out.format("\tActor: %s, Appearances: %d\n", actor._2, actor._1));
    }
}
