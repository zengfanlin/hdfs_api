package com.tedu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ProfitInfo implements WritableComparable<ProfitInfo> {
	private String name;
	private Long profit;

	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		profit = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
		out.writeLong(profit);
	}

	@Override
	public int compareTo(ProfitInfo o) {
		return (int) (o.getProfit() - profit);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getProfit() {
		return profit;
	}

	public void setProfit(Long profit) {
		this.profit = profit;
	}

	@Override
	public String toString() {
		return name + " " + profit ;
	}

}
