package com.tedu2;

import java.io.Serializable;

public class MetaData implements Serializable {
	private int startRow;
	private int endRow;

	public int getStartRow() {
		return startRow;
	}

	public MetaData(int startRow, int endRow) {
		super();
		this.startRow = startRow;
		this.endRow = endRow;
	}

	public void setStartRow(int startRow) {
		this.startRow = startRow;
	}

	public int getEndRow() {
		return endRow;
	}

	public void setEndRow(int endRow) {
		this.endRow = endRow;
	}

}
