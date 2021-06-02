package RddOperations;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.HashMap;

public class TopGenres {
    /**
     * Calculates the most popular genre for each decade
     * @param titleBasicsWithStartYear title.basics dataset without entries with null startYear
     * @return TopGenresRDD with decade as key and most popular genre as value
     */
    public static JavaPairRDD<Short, String> getTopGenresRDD(JavaRDD<Row> titleBasicsWithStartYear) {
        return titleBasicsWithStartYear // title.basics dataset without null startYear entries
                .filter(row -> !row.isNullAt(3)) // Genres cannot be null
                .mapToPair(row -> {
                    Short decade = (short) (Math.floor(row.getShort(2) / 10.0) * 10);
                    return new Tuple2<>(decade, row.getList(3));
                }) // Associate decade as key to genres list as values
                .flatMapValues(Iterable::iterator) // Expand genres
                .groupByKey() // Group by decade
                .mapValues(genres -> {
                    HashMap<String, Integer> counter = new HashMap<>();
                    genres.forEach(genre -> counter.merge((String) genre, 1, Integer::sum));
                    return counter.entrySet()
                            .stream()
                            .max((a, b) -> Integer.max(a.getValue(), b.getValue()))
                            .orElseThrow(() -> new Exception("No values!"))
                            .getKey();
                }); // Count genres and get max
    }
}
