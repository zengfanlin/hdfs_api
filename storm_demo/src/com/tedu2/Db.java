package com.tedu2;

public class Db {
	private  Db() {
	}

	private static String[] sentences = { "My name is park", "I am so shuai", "do you like me",
			"are you sure you do not like me", "ok i am sure", "how are you", "fine thank you and you",
			"i am fine thank you", "i am fine good bye" };

	public static String[] getData() {
		return sentences;
	}
}
