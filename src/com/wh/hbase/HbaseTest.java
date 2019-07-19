package com.wh.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseTest {
	@Test
	public void insert() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "192.168.1.10:2181,192.168.1.6:2181,192.168.1.7:2181");
		HTable table = new HTable(config, "tt");
		Put put = new Put(Bytes.toBytes("t0001"));
		put.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("tingting"));
		put.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(18));
		
		table.put(put);
		table.close();
		
	}
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "192.168.1.10:2181,192.168.1.6:2181,192.168.1.7:2181");
			HBaseAdmin admin = new HBaseAdmin(config);
			TableName name = TableName.valueOf("tt");
			
			HTableDescriptor  desc = new HTableDescriptor(name);
			HColumnDescriptor base_info = new HColumnDescriptor("base_info");
			HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
			base_info.setMaxVersions(5);
			desc.addFamily(base_info);
			desc.addFamily(extra_info);
			admin.createTable(desc);
			admin.close();
	}
}
