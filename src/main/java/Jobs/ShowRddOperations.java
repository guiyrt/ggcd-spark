package Jobs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class definition to execute second assignment objective
 */
public class ShowRddOperations {

    /**
     * Executes RDDs operations that satisfy the second objective of this assignment
     * @param args Input arguments
     */
    public static void main(String[] args) {
        // Instantiate spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ShowRddOperations")
                .config("hive.metastore", "thrift://hive-metastore:9083")
                .enableHiveSupport()
                .getOrCreate();

        // This first RDD will be used twice, so we can save the fetch and filter time by caching
        // All Datasets will select only the necessary columns
        JavaRDD<Row> titleBasicsWithStartYear = sparkSession
                .table("titleBasics")
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
        RddOperations.TopGenres
            .getTopGenresRDD(titleBasicsWithStartYear)
            .collect()
            .forEach(decade -> System.out.format("\tDecade: %d, Genre: %s\n", decade._1, decade._2));

        // Season Hits
        System.out.println("\nSeason Hits:");
        RddOperations.SeasonHits
            .getSeasonHitsRDD(titleRatings, titleBasicsWithStartYear)
            .collect()
            .forEach(season -> System.out.format("\tYear: %d, Title: %s, Rating: %s\n", season._1, season._2._1, season._2._2.toString()));

        // Top 10 Actors
        System.out.println("\nTop 10 Actors:");
        RddOperations.TopActors
            .getTopActorsRDD(titlePrincipals, nameBasics)
            .take(10)
            .forEach(actor -> System.out.format("\tActor: %s, Appearances: %d\n", actor._2, actor._1));
    }
}