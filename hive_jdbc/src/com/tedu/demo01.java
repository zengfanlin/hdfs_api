package com.tedu;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hive.common.jsonexplain.tez.Connection;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;
import org.junit.Test;

import com.mysql.jdbc.PreparedStatement;

public class demo01 {
	@Test
	public void main() throws ClassNotFoundException, SQLException {
		java.sql.Connection conn = null;
		java.sql.PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// 注册数据库驱动，用的hive的jdbc，驱动名固定写死
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			// 如果用的是hive2服务，则写jdbc:hive2，后面跟上hive服务器的ip以及端口号，端口号默认是10000
			conn = DriverManager.getConnection("jdbc:hive2://hadoop01:10000/jtdb", "root", "123456");
			//
			ps = conn.prepareStatement("select * from tb_book where id>?");
			ps.setInt(1, 2);
			rs = ps.executeQuery();
			while (rs.next()) {
				String name = rs.getString("name");
				System.out.println(name);
			}

		} catch (Exception e) {

			System.out.println(e.getStackTrace());
		} finally {
			rs.close();
			ps.close();
			conn.close();
		}

	}
}
