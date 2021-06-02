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

public class Actor implements Serializable {
    private final String name;
    private final Short age;
    private final Short activeYears;
    private final Long titles;
    private final BigDecimal avgRatings;
    private final List<String> friends;
    private final Map<Integer, String> generation = new HashMap<>();
    private final Map<Integer, String> hits = new HashMap<>();

    public static void main(String[] args) throws IOException {
        // HBase config
        Configuration conf = HBaseConfiguration.create();

        // Get table from HBase
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("actorPage"));

        // Print example of actor from actorPage
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(new Actor(table, "nm0244782")));
    }

    public Actor(Table table, String key) throws IOException {
        Result r = table.get(new Get(Bytes.toBytes(key)));

        name = Bytes.toString(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("name")));
        age = Bytes.toShort(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("age")));
        activeYears = Bytes.toShort(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("active_years")));
        titles = Bytes.toLong(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("titles")));
        avgRatings = Bytes.toBigDecimal(r.getValue(Bytes.toBytes("base"), Bytes.toBytes("avg_ratings")));
        friends = Arrays.asList(Bytes.toString(r.getValue(Bytes.toBytes("friends"), Bytes.toBytes("friends"))).split("\t"));
        IntStream.range(0,10).forEach(i -> generation.put(i, Bytes.toString(r.getValue(Bytes.toBytes("generation"), Bytes.toBytes(Integer.toString(i))))));
        IntStream.range(0,10).forEach(i -> hits.put(i, Bytes.toString(r.getValue(Bytes.toBytes("hits"), Bytes.toBytes(Integer.toString(i))))));
    }

    public String getName() {
        return name;
    }

    public Short getAge() {
        return age;
    }

    public Short getActiveYears() {
        return activeYears;
    }

    public Long getTitles() {
        return titles;
    }

    public BigDecimal getAvgRatings() {
        return avgRatings;
    }

    public List<String> getFriends() {
        return friends;
    }

    public Map<Integer, String> getGeneration() {
        return generation;
    }

    public Map<Integer, String> getHits() {
        return hits;
    }
}