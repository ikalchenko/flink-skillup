package me.ikalchenko.flink;

import org.apache.flink.table.api.*;

import java.nio.file.Paths;

public class ImdbJob {
	static final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
	static String filePrefix;


	public static void main(String[] args) {
		filePrefix = System.getenv("FILE_PREFIX");
		TaskOne t1 = new TaskOne();
		t1.getResult();


	}
}
