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

public class HBase {
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

    public static void insertActorInfo(Table table, Row baseAndFriends, List<String> hits, List<String> generation) throws IOException {
        Put row = new Put(Bytes.toBytes(baseAndFriends.getString(0)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes(baseAndFriends.getString(1)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("age"), Bytes.toBytes(baseAndFriends.getShort(2)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("active_years"), baseAndFriends.isNullAt(4) ?
                Bytes.toBytes((short) 0) :
                Bytes.toBytes(baseAndFriends.getShort(4)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("titles"), Bytes.toBytes(baseAndFriends.getLong(5)));
        row.addColumn(Bytes.toBytes("base"), Bytes.toBytes("avg_ratings"), Bytes.toBytes(baseAndFriends.getDecimal(6)));
        row.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("friends"), Bytes.toBytes(String.join("\t", baseAndFriends.getList(7))));

        ListIterator<String> hitsIt = hits.listIterator();
        while (hitsIt.hasNext()) {
            row.addColumn(Bytes.toBytes("hits"), Bytes.toBytes(Integer.toString(hitsIt.nextIndex())), Bytes.toBytes(hitsIt.next()));
        }

        ListIterator<String> genIt = generation.listIterator();
        while (genIt.hasNext()) {
            row.addColumn(Bytes.toBytes("generation"), Bytes.toBytes(Integer.toString(genIt.nextIndex())), Bytes.toBytes(genIt.next()));
        }

        table.put(row);
    }
}