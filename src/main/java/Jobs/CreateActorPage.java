package Jobs;

import ActorPage.HBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class definition to execute third assignment objective
 */
public class CreateActorPage {

    /**
     * Executes RDDs and SQL operations to satisfy the third objective of this assignment
     * actorPage data is stored in HBase, which can be retrieved using the Actor class
     * @param args Input arguments
     */
    public static void main(String[] args) throws IOException {
        // HBase config
        Configuration conf = HBaseConfiguration.create();

        // Instantiate spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CreateActorPage")
                .config("hive.metastore", "thrift://hive-metastore:9083")
                .enableHiveSupport()
                .getOrCreate();

        // Create actorPage HBase table
        HBase.createTableIfNotTaken(conf, "actorPage", Arrays.asList("base", "friends", "generation", "hits"));

        // Base Dataset
        Dataset<Row> dataset = ActorPage.Base.getBaseDataset(sparkSession);

        // Add friends Dataset
        dataset = dataset.join(ActorPage.Friends.getFriendsDataset(sparkSession), "nconst");

        // Join with Hits RDD
        Iterator<Tuple2<Row, List<String>>> actorData = dataset
            .toJavaRDD() // Convert to RDD to join with hits
            .mapToPair(row -> new Tuple2<>(row.getString(0), row)) // Convert to PairRDD with actor id as key, and base + friends as value
            .join(ActorPage.Hits.getHitsRDD(sparkSession)) // Join on actor id
            .map(Tuple2::_2) // Actor id is present in base + friends, so discard actor id key
            .toLocalIterator(); // Convert to iterator

        // Generation RDD to map
        Map<Short, List<String>> generations = ActorPage.Generation.getGenerationRDD(sparkSession).collectAsMap();

        // Get table from HBase
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("actorPage"));

        // Iterate through actors
        while (actorData.hasNext()) {
            // Get actor and corresponding top generation actors
            Tuple2<Row, List<String>> actor = actorData.next();
            List<String> generation = generations.get(actor._1.getShort(3));

            // Add to table
            HBase.insertActorInfo(table, actor._1, actor._2, generation);
        }

        // Close connections
        conn.close();
        table.close();
    }
}