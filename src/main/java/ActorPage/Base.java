package ActorPage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Base {
    public static Dataset<Row> getBaseDataset(SparkSession sparkSession) {
        return sparkSession.sql(
                "select nb.nconst," +
                "       nb.primaryName," +
                "       cast((nvl(nb.deathyear, date_format(current_date, 'yyyy')) - nb.birthyear) as smallint) as age," +
                "       cast(cast(nb.birthyear/10 as int)*10 as smallint) as decade, " +
                "       cast((max(tb.startyear) - min(tb.startyear)) as smallint) as active_years," +
                "       count(tb.tconst) as titles," +
                "       cast(avg(r.averagerating) as decimal(10,2)) as avg_ratings\n" +
                "from namebasics nb" +
                "         join titleprincipals p on nb.nconst == p.nconst" +
                "         join titlebasics tb on p.tconst = tb.tconst" +
                "         join titleratings r on tb.tconst = r.tconst\n" +
                "where(array_contains(nb.primaryprofession, 'actress') or array_contains(nb.primaryprofession, 'actor'))" +
                "   and nb.birthyear is not null " +
                "group by nb.nconst, nb.primaryName, nb.birthyear, nb.deathyear;"
        );
    }
}