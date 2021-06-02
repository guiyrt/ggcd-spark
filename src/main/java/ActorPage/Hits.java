package ActorPage;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Class definition to get hits information
 */
public class Hits {

    /**
     * SQL and RDD operations to get hits information for actorPage
     * @param sparkSession Connection to Hive
     * @return Hits information for actorPage
     */
    public static JavaPairRDD<String, List<String>> getHitsRDD(SparkSession sparkSession) {
        return sparkSession.sql(
                "select nb.nconst," +
                        "tb.primarytitle," +
                        "r.averagerating\n" +
                        "from namebasics nb" +
                        "    join titleprincipals p on nb.nconst == p.nconst" +
                        "    join titleratings r on p.tconst = r.tconst" +
                        "    join titlebasics tb on p.tconst = tb.tconst\n" +
                        "where(array_contains(nb.primaryprofession, 'actress') or array_contains(nb.primaryprofession, 'actor')) " +
                        "   and nb.birthyear is not null;") // SQL query to get titles with ratings associated with actor id
                .toJavaRDD() // Convert to RDD
                .mapToPair(row ->
                        new Tuple2<>(
                                row.getString(0),
                                new Tuple2<>(
                                        row.getString(1),
                                        row.getDecimal(2)
                ))) // Convert to PairRDD, using actor id as key, and tuples of title name and rating as values
                .groupByKey() // Group by actor id
                .mapValues(titles -> {
                    TreeSet<Tuple2<String, BigDecimal>> titlesWithRating = new TreeSet<>(Comparator.comparing((Tuple2<String, BigDecimal> a) -> a._2).reversed());
                    titles.forEach(titlesWithRating::add);
                    return titlesWithRating.stream().limit(10).map(Tuple2::_1).collect(Collectors.toList());
                }); // Order values by ratings and get top 10 title names
    }
}