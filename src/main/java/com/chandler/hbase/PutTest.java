package com.chandler.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutTest {

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "chandler002");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        String tableNameStr = "test1";

        try (Connection conn = ConnectionFactory.createConnection(conf);
        ) {
            Table table = conn.getTable(TableName.valueOf(tableNameStr));
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("chandler"));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
