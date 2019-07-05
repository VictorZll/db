package com.newroad.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**
 * 使用远程
 * @author Administrator
 *
 */
public class demo2 {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的远程程序demo2";
		String master="spark://zll:7077";
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("num.executors", "4");
		conf.set("executor-cores", "2");
		conf.set("executor-memory", "500M");
		//conf.set("spark.ui.killEnabled","false");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		
		JavaRDD<String> rdd=sc.textFile("hdfs://zll:9000/26.txt");
		// rdd1 = sc.hadoopFile("hdfs://zll:9000/26.txt", String.class, String.class, String.class);
		
		long count =rdd.count();
		System.out.println("第一行"+rdd.first());
		System.out.println("count="+count);
		Thread.sleep(8000);
		sc.stop();
	}

}









