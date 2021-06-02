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

public class CreateActorPage {
    public static void main(String[] args) throws IOException {
        // HBase config
        Configuration conf = HBaseConfiguration.create();

        // Instantiate spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ShowRddOperations")
                .config("hive.metastore", "thrift://hive-metastore:9083")
                .enableHiveSupport()
                .getOrCreate();

        // Create HBase table
        HBase.createTableIfNotTaken(conf, "actorPage", Arrays.asList("base", "friends", "generation", "hits"));

        // Base Dataset
        Dataset<Row> dataset = ActorPage.Base.getBaseDataset(sparkSession);

        // Add friends Dataset
        dataset = dataset.join(ActorPage.Friends.getFriendsDataset(sparkSession), "nconst");

        // Join with Hits RDD
        Iterator<Tuple2<Row, List<String>>> actorData = dataset
            .toJavaRDD()
            .mapToPair(row -> new Tuple2<>(row.getString(0), row))
            .join(ActorPage.Hits.getHitsRDD(sparkSession))
            .map(Tuple2::_2)
            .toLocalIterator();

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