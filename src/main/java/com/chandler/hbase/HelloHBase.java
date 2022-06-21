package com.chandler.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.hadoop.hbase.HConstants.ALL_VERSIONS;

public class HelloHBase {

    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "chandler002");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        String tableNameStr = "test1";

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin();
        ) {
            TableName table = TableName.valueOf(tableNameStr);
            if (admin.isTableAvailable(table)) {
                //表存在，先删除
                admin.disableTable(table);
                admin.deleteTable(table);
                System.out.println("删除table");
            }
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(table);
            //列族描述起构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes("user"))
                    .setCompactionCompressionType(Compression.Algorithm.GZ)
                    .setMaxVersions(ALL_VERSIONS);
            ColumnFamilyDescriptor cfd = cdb.build();

            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
            System.out.println("建表成功");
        }
    }
}
