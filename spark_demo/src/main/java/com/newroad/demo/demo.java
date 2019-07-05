package com.newroad.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**
 * 使用本地
 * @author Administrator
 *
 */
public class demo {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序";
		String master="local[4]";
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("spark.ui.killEnabled","false");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> rdd=sc.textFile("E:\\test.md");
		long count =rdd.count();
		System.out.println("第一行"+rdd.first());
		System.out.println("count="+count);
		Thread.sleep(8000000);
		sc.stop();
	}

}
