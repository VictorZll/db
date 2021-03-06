package com.newroad.demo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
/**
 * 匿名内部类
 * @author Administrator
 *
 */
public class demo4 {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序demo4";
		String master="spark://192.168.8.133:7077";
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("num.executors", "4");
		conf.set("executor-cores", "2");
		conf.set("executor-memory", "500M");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> rdd=sc.textFile(args[0]);
		//1行-》多行
		rdd=rdd.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				String[] arrs=t.split(",");
				List<String> list=Arrays.asList(arrs);
				return list.iterator();
			}
		});
		//rdd.saveAsTextFile(args[1]);
		
		String sum=rdd.reduce(new Function2<String, String, String>() {
			
			@Override
			public String call(String v1, String v2) throws Exception {
				
				return Integer.valueOf(v1)+Integer.valueOf(v2)+"";
			}
		});
		System.out.println("sum="+sum);

		sc.stop();
	}

}
