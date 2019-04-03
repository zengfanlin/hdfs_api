package com.tedugroup;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class WCGroupingComparator extends WritableComparator {
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// 统计a-n 放一起 o-z开头的放一起
		// 不能使用new string原因是传过来的内容是序列化的，解析的时候需要反
		Text kaText = new Text();
		Text kbText = new Text();
		// bytes到text
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(b1, s1, l1));
		try {
			kaText.readFields(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		in = new DataInputStream(new ByteArrayInputStream(b2, s2, l2));
		try {
			kbText.readFields(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 自定义分组[a-n]一组 o-z一组
		if (kaText.toString().matches("[a-n][a-z]*") && kbText.toString().matches("[a-n][a-z]*")) {
			return 0;
		} else if (kaText.toString().matches("[o-z][a-z]*") && kbText.toString().matches("[o-z][a-z]*")) {
			return 0;
		} else {
			return -1;
		}
	}
}
