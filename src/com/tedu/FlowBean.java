package com.tedu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable {
	private String phone;
	private String addr;
	private String name;
	private long flow;

	public FlowBean() {

	}

	public FlowBean(String phone, String addr, String name, long flow) {
		super();
		this.phone = phone;
		this.addr = addr;
		this.name = name;
		this.flow = flow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		phone = in.readUTF();
		addr = in.readUTF();
		name = in.readUTF();
		flow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeUTF(addr);
		out.writeUTF(name);
		out.writeLong(flow);
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getFlow() {
		return flow;
	}

	public void setFlow(long flow) {
		this.flow = flow;
	}

}
