package ActorPage;

import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Class to fetch data from actorPage table in HBase
 */
public class Actor implements Serializable {

    // actorPage attributes
    private final String name;
    private final Short age;
    private final Short activeYears;
    private final Long titles;
    private final BigDecimal avgRatings;
    private final List<String> friends;
    private final Map<Integer, String> generation = new HashMap<>();
    private final Map<Integer, String> hits = new HashMap<>();

    /**
     * Example of fetching and printing actor information from HBase
     * @param args Input arguments
     * @throws IOException HBase related operations
     */
    public static void main(String[] args) throws IOException {
        // HBase config
        Configuration conf = HBaseConfiguration.create();

        // Get table from HBase
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("actorPage"));

        // Print example of Robert Downey Jr. from actorPage
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(new Actor(table, "nm0000375")));
    }

    /**
     * Constructor that goes directly to HBase to get actorPage data
     * @param table actorPage table as argument, to minimize new connections
     * @param key actor key of data to fetch
     * @throws IOException HBase related operations
     */
    public Actor(Table table, String key) throws IOException {
        // Get row from table
        Result r = table.get(new Get(Bytes.toBytes(key)));

        // Get actor attributes from row result
        name = Bytes.toString(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("name")));
        age = Bytes.toShort(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("age")));
        activeYears = Bytes.toShort(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("active_years")));
        titles = Bytes.toLong(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("titles")));
        avgRatings = Bytes.toBigDecimal(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("avg_ratings")));
        friends = Arrays.asList(Bytes.toString(r.getValue(Bytes.toBytes("friends"), Bytes.toBytes("friends"))).split("\t"));
        IntStream.range(0,10).forEach(i -> generation.put(i, Bytes.toString(r.getValue(Bytes.toBytes("generation"), Bytes.toBytes(Integer.toString(i))))));
        IntStream.range(0,10).forEach(i -> hits.put(i, Bytes.toString(r.getValue(Bytes.toBytes("hits"), Bytes.toBytes(Integer.toString(i))))));
    }

    /**
     * Name getter
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Age getter
     * @return age
     */
    public Short getAge() {
        return age;
    }

    /**
     * activeYears getter
     * @return activeYears
     */
    public Short getActiveYears() {
        return activeYears;
    }

    /**
     * titles getter
     * @return number of titles
     */
    public Long getTitles() {
        return titles;
    }

    /**
     * avgRatings getter
     * @return avgRatings
     */
    public BigDecimal getAvgRatings() {
        return avgRatings;
    }

    /**
     * friends getter
     * @return friends list
     */
    public List<String> getFriends() {
        return friends;
    }

    /**
     * generation getter
     * @return generation top 10 actors list
     */
    public Map<Integer, String> getGeneration() {
        return generation;
    }

    /**
     * hits getter
     * @return hits top 10 titles list
     */
    public Map<Integer, String> getHits() {
        return hits;
    }
}