package ActorPage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * Class definition to handle operations with HBase
 */
public class HBase {

    /**
     * Creates a table if name is available
     * @param conf HBase configuration to create connection
     * @param tableName Table to create
     * @param cfs Collection of column families
     * @throws IOException HBase related operations
     */
    public static void createTableIfNotTaken(Configuration conf, String tableName, Collection<String> cfs) throws IOException {
        // Instantiating Admin class
        Admin admin = ConnectionFactory.createConnection(conf).getAdmin();

        // Check if table is free
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            // Create table builder
            TableDescriptorBuilder table = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

            // Add column families
            for (String cf : cfs) {
                table.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes(StandardCharsets.UTF_8)).build());
            }

            // Create table
            admin.createTable(table.build());
        }

        // End connection
        admin.close();
    }

    /**
     * Inserts actor information into actorPage table in HBase
     * @param table actorPage table as argument, to minimize new connections
     * @param baseAndFriends Row with base and friends information
     * @param hits List of actor top 10 title hits
     * @param generation List of top 10 actors born in the same decade as the actor (by average ratings)
     * @throws IOException HBase related operations
     */
    public static void insertActorInfo(Table table, Row baseAndFriends, List<String> hits, List<String> generation) throws IOException {
        // Create Put instance for actor identifier
        Put row = new Put(Bytes.toBytes(baseAndFriends.getString(0)));

        // Add actor base and friends attributes
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes(baseAndFriends.getString(1)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("age"), Bytes.toBytes(baseAndFriends.getShort(2)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("active_years"), baseAndFriends.isNullAt(4) ?
                Bytes.toBytes((short) 0) :
                Bytes.toBytes(baseAndFriends.getShort(4)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("titles"), Bytes.toBytes(baseAndFriends.getLong(5)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("avg_ratings"), Bytes.toBytes(baseAndFriends.getDecimal(6)));
        row.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("friends"), Bytes.toBytes(String.join("\t", baseAndFriends.getList(7))));

        // Add actor hits attribute
        ListIterator<String> hitsIt = hits.listIterator();
        while (hitsIt.hasNext()) {
            row.addColumn(Bytes.toBytes("hits"), Bytes.toBytes(Integer.toString(hitsIt.nextIndex())), Bytes.toBytes(hitsIt.next()));
        }

        // Add actor generation attribute
        ListIterator<String> genIt = generation.listIterator();
        while (genIt.hasNext()) {
            row.addColumn(Bytes.toBytes("generation"), Bytes.toBytes(Integer.toString(genIt.nextIndex())), Bytes.toBytes(genIt.next()));
        }

        // Insert actor information into table
        table.put(row);
    }
}