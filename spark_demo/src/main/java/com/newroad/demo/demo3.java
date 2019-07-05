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
public class demo3 {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序demo3";
		String master="spark://192.168.8.133:7077";
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("num.executors", "4");
		conf.set("executor-cores", "2");
		conf.set("executor-memory", "500M");
		//conf.set("spark.ui.killEnabled","false");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		//hdfs://zll:9000/hx.txt
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
		JavaRDD<Integer> rddInt=rdd.map(new Function<String, Integer>() {

			@Override
			public Integer call(String v1) throws Exception {
				if(v1.contains(args[2])) {
					System.out.println(v1);
					return 1;
				}
			
				return 0;
			}
		});
	
		// rdd1 = sc.hadoopFile("hdfs://zll:9000/26.txt", String.class, String.class, String.class);
		//hdfs://zll:9000/result.txt
		//rddInt.saveAsTextFile(args[1]);
		rdd.saveAsTextFile(args[1]);
		//long count =rdd.count();
		//System.out.println("第一行"+rdd.first());
		//System.out.println("count="+count);
		
		String sum=rdd.reduce(new Function2<String, String, String>() {
			
			@Override
			public String call(String v1, String v2) throws Exception {
				
				return Integer.valueOf(v1)+Integer.valueOf(v2)+"";
			}
		});
		System.out.println("sum="+sum);
		
//		List<Integer> tt=rddInt.collect();
//		for(Integer t:tt) {
//			System.out.println("-->"+t);
//		}
		
		//Thread.sleep(8000);
		sc.stop();
	}

}
