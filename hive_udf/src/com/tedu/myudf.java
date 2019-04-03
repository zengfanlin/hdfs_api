package com.tedu;

import org.apache.hadoop.hive.ql.exec.UDF;

public class myudf extends UDF {
	public String evaluate(String s) {
		return s.toUpperCase();
	}
}
