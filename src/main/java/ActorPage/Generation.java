package ActorPage;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class Generation {
    public static JavaPairRDD<Short, List<String>> getGenerationRDD(SparkSession sparkSession) {
        return sparkSession.sql("select cast(cast(nb.birthyear/10 as int)*10 as smallint) as decade," +
                "       nb.primaryname," +
                "       avg(r.averagerating) as average_rating\n" +
                "from namebasics nb" +
                "    join titleprincipals p on nb.nconst == p.nconst" +
                "    join titleratings r on p.tconst = r.tconst" +
                "    join titlebasics tb on p.tconst = tb.tconst\n" +
                "where(array_contains(nb.primaryprofession, 'actress') or array_contains(nb.primaryprofession, 'actor')) " +
                "    and nb.birthyear is not null\n" +
                "group by nb.birthyear, nb.primaryname;")
                .toJavaRDD()
                .mapToPair(row ->
                        new Tuple2<>(row.getShort(0),
                                new Tuple2<>(row.getString(1), row.getDecimal(2))))
                .groupByKey()
                .mapValues(actors -> {
                        TreeSet<Tuple2<String, BigDecimal >> actorsWithRating = new TreeSet<>(Comparator.comparing((Tuple2<String, BigDecimal> a) -> a._2).reversed());
                        actors.forEach(actorsWithRating::add);
                        return actorsWithRating.stream().limit(10).map(Tuple2::_1).collect(Collectors.toList());
                });
    }
}