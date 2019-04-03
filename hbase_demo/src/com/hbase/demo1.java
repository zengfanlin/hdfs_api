package com.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.VAR;
import org.junit.jupiter.api.Test;

import com.sun.org.apache.bcel.internal.generic.NEW;

public class demo1 {
	@Test
	public void name() {
		System.out.println("1111111111111");
	}

	@Test
	public void testCreate() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(config);
		// 创建表：
		HTableDescriptor tab1 = new HTableDescriptor(org.apache.hadoop.hbase.TableName.valueOf("tab1"));
		// 指定列族名：
		HColumnDescriptor cf1 = new HColumnDescriptor("colfam1".getBytes());
		HColumnDescriptor cf2 = new HColumnDescriptor("colfam2".getBytes());
		cf1.setMaxVersions(3);
		tab1.addFamily(cf1);
		tab1.addFamily(cf2);
		admin.createTable(tab1);
		admin.close();
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testInsert() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		HTable table = new HTable(config, "tab1");
		Put put = new Put("row-1".getBytes());
		// 列族，列,值
		put.add("colfam1".getBytes(), "col1".getBytes(), "aaa".getBytes());
		put.add("colfam1".getBytes(), "col2".getBytes(), "bbb".getBytes());
		table.put(put);
		table.close();
	}

	@Test
	public void testInsertMillion() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		@SuppressWarnings("deprecation")
		HTable table = new HTable(config, "tab1");

		List<Put> puts = new ArrayList<>();

		long begin = System.currentTimeMillis();

		for (int i = 1; i < 1000000; i++) {
			Put put = new Put(("row" + i).getBytes());
			put.add("colfam1".getBytes(), "col".getBytes(), ("" + i).getBytes());
			puts.add(put);

			// 批处理，批大小为:10000
			if (i % 10000 == 0) {
				table.put(puts);
				puts = new ArrayList<>();
			}
		}
		table.close();
		long end = System.currentTimeMillis();// 18183
		System.out.println(end - begin);
	}

	@Test
	public void testGet() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		HTable table = new HTable(config, "tab1");
		Get get = new Get("row1".getBytes());
		Result result = table.get(get);
		byte[] col1_result = result.getValue("colfam1".getBytes(), "col".getBytes());
		System.out.println(new String(col1_result));
		table.close();
	}

//	获取数据集：
	@Test
	public void testScan() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		HTable table = new HTable(config, "tab1");
		// 获取row100及以后的行键的值
		Scan scan = new Scan("row100".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		Iterator it = scanner.iterator();
//		while (it.hasNext()) {
//			Result result = (Result) it.next();
		// 指定行键
//			byte[] bs = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("col"));
//			String str = Bytes.toString(bs);
//			System.out.println(str);
//		}

		while (it.hasNext()) {
			StringBuffer sb = new StringBuffer();
			Result result = (Result) it.next();
			String rk = new String(result.getRow());
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();// 获取列族
			for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {// 获取列族的列
				String cf = new String(entry.getKey());
				NavigableMap<byte[], NavigableMap<Long, byte[]>> cmap = entry.getValue();
				for (Entry<byte[], NavigableMap<Long, byte[]>> centry : cmap.entrySet()) {
					String ckey = new String(centry.getKey());
					String cv = new String(centry.getValue().firstEntry().getValue());// 获取最新version的值
					sb.append(rk + "=" + cf + "-" + ckey + "-" + cv);
				}
			}
			System.out.println(sb.toString());

		}

		table.close();

	}

	@Test
	public void testDelete() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		HTable table = new HTable(config, "tab1");
		Delete delete = new Delete("row1".getBytes());
		table.delete(delete);
		table.close();

	}

	@Test
	public void testDeleteTable() throws Exception, IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable("tab1".getBytes());
		admin.deleteTable("tab1".getBytes());
		admin.close();
	}

	@Test
	public void testScanner() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		HTable table = new HTable(conf, "ns1:tab3".getBytes());
		Scan scan = new Scan();
		scan.setStartRow("rk3".getBytes());
		scan.setStopRow("rk5".getBytes());

		ResultScanner rs = table.getScanner(scan);

		Result r = null;
		while ((r = rs.next()) != null) {
			String rowKey = new String(r.getRow());
			String col1Value = new String(r.getValue("cf1".getBytes(), "c1".getBytes()));
			System.out.println(rowKey + ":" + col1Value);
		}

	}

	@Test
	public void scanData() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		HTable table = new HTable(conf, "ns1:tab3".getBytes());

		Scan scan = new Scan();

		// --正则过滤器，匹配行键含3的行数据
//		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^.*3.*$"));
		//--行键比较过滤器，下例是匹配小于或等于指定行键的行数据。
//		Filter filter=new RowFilter(CompareOp.LESS_OR_EQUAL,new BinaryComparator("row90".getBytes()));
		//--行键前缀过滤器
		Filter filter=new PrefixFilter("r".getBytes());
		//--列值过滤器
//		Filter filter = new SingleColumnValueFilter("cf1".getBytes(),"name".getBytes(), CompareOp.EQUAL, "rose".getBytes());

		// --加入过滤器
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);
		// --获取结果的迭代器
		Iterator<Result> it = scanner.iterator();
		while (it.hasNext()) {
			Result result = it.next();
			// --通过result对象获取指定列族的列的数据
			byte[] value = result.getValue("cf1".getBytes(), "c1".getBytes());
			System.out.println(new String(value));
		}
		scanner.close();
	}

}
