package com.newroad.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 *  spark_session 本地运算
 * @author Administrator
 *
 */
public class demo9 {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序demo9";
		String master="local";
		String file_path=args[0];
		
		SparkSession sks=SparkSession.builder().master(master).appName(appName).config("spark.some.config.option", "some-value").getOrCreate();
		Dataset<String> df=sks.read().textFile(file_path);
		df.show(100);
	

	}

}
