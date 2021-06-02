package ActorPage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class definition to get friends information
 */
public class Friends {

    /**
     * SQL operation to get friends information for actorPage
     * @param sparkSession Connection to Hive
     * @return Friends information for actorPage
     */
    public static Dataset<Row> getFriendsDataset(SparkSession sparkSession) {
        return sparkSession.sql("select nb.nconst," +
                "    collect_set(nb2.primaryname) as friends\n" +
                "from namebasics nb" +
                "   join titleprincipals p on nb.nconst == p.nconst" +
                "   join titleprincipals p2 on (p.tconst = p2.tconst and nb.nconst != p2.nconst)" +
                "   join namebasics nb2 on (p2.nconst == nb2.nconst)" +
                "where (array_contains(nb.primaryprofession, 'actress') or array_contains(nb.primaryprofession, 'actor'))" +
                "   and nb.birthyear is not null " +
                "   and ((p2.category = 'actress') or (p2.category = 'actor'))" +
                "group by nb.nconst;");
    }
}