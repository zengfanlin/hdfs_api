package com.tedu;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.VAR;
import org.junit.jupiter.api.Test;

public class demo1 {
	@Test
	public void name() throws Exception {
		java.sql.Connection conn = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
//			Driver
			conn = DriverManager.getConnection("jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181");
			statement = conn.prepareStatement("select * from \"tabx3\" where \"id\"=?");
			statement.setInt(1, 2);
			rs = statement.executeQuery();
			while (rs.next()) {
				String name = rs.getString("name");
				System.out.println(name);
			}
		} catch (Exception e) {
			// TODO: handle exception
			rs.close();
			conn.close();
		} finally {
			rs = null;
			conn = null;
		}
	}
}
