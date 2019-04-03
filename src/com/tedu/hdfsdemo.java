package com.tedu;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Options.FSDataInputStreamOption;
import org.junit.Test;

public class hdfsdemo {

	@Test
	public void testconnect() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
//		conf.set("dfs.replicaton", "1");//指定副本数量
		// FileSystemhadoop文件系统抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop11:9000"), conf);
		System.out.println(fs);
		fs.close();
	}

	@Test
	public void mkdir() throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		// FileSystemhadoop文件系统抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop11:9000"), conf, "root");
		System.out.println(fs);
		fs.mkdirs(new Path("/park/word/"));
		fs.close();
	}

	/**
	 * hdfs上上传到本地
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	@Test
	public void testcopytolocal() throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		// FileSystemhadoop文件系统抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop11:9000"), conf, "root");
		FSDataInputStream fsin = fs.open(new Path("/data/book.txt"));
		OutputStream out = new FileOutputStream(new File("1.txt"));
		IOUtils.copyBytes(fsin, out, 1024);
		out.close();
		fsin.close();
		fs.close();
	}

	/**
	 * 本地上传hdfs上
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	@Test
	public void testcopyfromlocal() throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");// 指定副本数量()
		// FileSystemhadoop文件系统抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop11:9000"), conf, "root");
		FSDataOutputStream out = fs.create(new Path("/park/word/words.txt"));
		FileInputStream in = new FileInputStream(new File("words.txt"));
		IOUtils.copyBytes(in, out, 1024);
		out.close();
		in.close();
		fs.close();
	}

	/**
	 * 获取块的大小
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	@Test
	public void getblocklocation() throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		// FileSystemhadoop文件系统抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop11:9000"), conf, "root");
		BlockLocation[] bls = fs.getFileBlockLocations(new Path("/data/book.txt"), 0, Long.MAX_VALUE);
		for (BlockLocation blockLocation : bls) {
			System.out.println(blockLocation);
		}
		fs.close();
	}
}
