package com.newroad.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
/**
 * 统计频率
 * @author Administrator
 *
 */
public class demo5 {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序demo5";
		String master="spark://192.168.8.133:7077";
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("num.executors", "4");
		conf.set("executor-cores", "2");
		conf.set("executor-memory", "500M");
		JavaSparkContext sc=new JavaSparkContext(conf);
		

		JavaRDD<String> rdd=sc.textFile(args[0]);
		//String->k,v
		JavaPairRDD<String, Integer> rddToPair=
				rdd.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			@Override
			public Iterator<Tuple2<String, Integer>> call(String t) throws Exception {
				System.out.println("读取的行是---------->"+t);
				String[] arrs=t.split(",");
				List<Tuple2<String, Integer>> list=new ArrayList<Tuple2<String,Integer>>();
				for(String arr: arrs) {
					list.add(new Tuple2<String, Integer>(arr, 1));
				}
				return list.iterator();
			}
		});
		//累加
		rddToPair=rddToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
			
				return v1.intValue()+v2.intValue();
			}
		});
		rddToPair.saveAsTextFile(args[1]);
		sc.stop();
	}

}
