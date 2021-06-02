package RddOperations;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public class TopActors {
    /**
     * Calculate the title appearances for each actor/actress
     * @param titlePrincipals title.principals dataset
     * @param nameBasics name.basics dataset
     * @return TopActorsRDD with appearances as key and name as value
     */
    public static JavaPairRDD<Integer, String> getTopActorsRDD(JavaRDD<Row> titlePrincipals, JavaRDD<Row> nameBasics) {
         return titlePrincipals // title.principals dataset
                .filter(title -> title.getString(2).equals("actor") || title.getString(2).equals("actress")) // Just keep actors/actresses
                .mapToPair(title -> new Tuple2<>(title.getString(1), 1)) // Prepare to count
                .reduceByKey(Integer::sum) // Count actor/actress appearances
                .join(nameBasics.mapToPair(name -> new Tuple2<>(name.getString(0), name.getString(1)))) // Join with name.basics to get names
                .mapToPair(Tuple2::_2) // Keep appearances and name
                .sortByKey(false); // Sort by descending order of appearances
    }
}
