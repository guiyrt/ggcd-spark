package Actions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Iterator;

public class SeasonHits {
    /**
     * Calculate title with best rating for each year
     * @param titleRatings title.ratings dataset
     * @param titleBasicsWithStartYear title.basics dataset without entries with null startYear
     * @return SeasonHitsRDD with year as key, and tuple of name and rating as value
     */
    public static JavaPairRDD<Short, Tuple2<String, BigDecimal>> getSeasonHitsRDD(JavaRDD<Row> titleRatings, JavaRDD<Row> titleBasicsWithStartYear) {
        return titleRatings // title.ratings dataset from parquet representation
                .mapToPair(row -> new Tuple2<>(row.getString(0), row)) // Prepare to join with title.basics, tconst as key
                .join(titleBasicsWithStartYear.mapToPair(row -> new Tuple2<>(row.getString(0), row))) // Join with title.basics with tconst as key
                .mapToPair(title -> new Tuple2<>(
                        title._2._2.getShort(2),
                        new Tuple2<>(
                                title._2._2.getString(1),
                                title._2._1.getDecimal(1)
                        )
                    )
                ) // Use year as key, and tuple of name and rating as value
                .groupByKey() // Group by year
                .mapValues(titles -> {
                    Iterator<Tuple2<String, BigDecimal>> it = titles.iterator();
                    Tuple2<String, BigDecimal> maxTitle = it.next();

                    for(Tuple2<String, BigDecimal> title: titles) {
                        if (title._2.compareTo(maxTitle._2)>0) {
                            maxTitle = title;
                        }
                    }
                    return maxTitle;
                }); // Go through titles and get one with max rating
    }
}
