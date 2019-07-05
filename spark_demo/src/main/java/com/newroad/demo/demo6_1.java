package com.newroad.demo;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.newroad.entity.Keys;
import com.newroad.entity.User;

import scala.Tuple2;
/**
 * 统计频率 过滤 --------------使用partitions优化
 * @author Administrator
 *
 */
public class demo6_1 {
	/**
	 * 不使用
	 * @param hdfs_path
	 * @throws Exception
	 */
	public static void testDeleteFile(String hdfs_path) throws Exception {
		Path path=new Path(hdfs_path);
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://zll:9000"),conf,"root");
		if(fs.exists(path)) {
			fs.delete(new Path(hdfs_path), false);
			System.out.println(" hadoop delete " + hdfs_path + " ok !!!");
		}
	}
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序demo6";
		String master="local";
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("num.executors", "4");
		conf.set("executor-cores", "2");
		conf.set("executor-memory", "500M");
		JavaSparkContext sc=new JavaSparkContext(conf);
		

		JavaRDD<String> rdd=sc.textFile(args[0]);
		rdd.repartition(5);
		//String->k,v
		JavaPairRDD<Keys, User> rddToPair=rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Keys, User>() {

			@Override
			public Iterator<Tuple2<Keys, User>> call(Iterator<String> t) throws Exception {
				int count=0;
				//System.out.println("读取的行是---------->"+t);
				List<Tuple2<Keys, User>> list=new ArrayList<Tuple2<Keys, User>>();
				User u=new User();
				Keys k=new Keys();
			
				while(t.hasNext()) {
					count++;
					StringTokenizer st=new StringTokenizer(t.next());
					while(st.hasMoreTokens()) {
						
						
						u.setUploader(st.nextToken());
						u.setVideos(Integer.valueOf(st.nextToken()));
						u.setFriends(Integer.valueOf(st.nextToken()));
						k.setFriends(u.getFriends());
						k.setVideos(u.getVideos());
						
					}
					list.add(new Tuple2<Keys, User>(k, u));
				}
				System.out.println("count==========" + count);
				return list.iterator();
				
				
			}

		});
//		JavaPairRDD<String, Integer> rddToPair=
//				rdd.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
//
//			@Override
//			public Iterator<Tuple2<String, Integer>> call(String t) throws Exception {
//				System.out.println("读取的行是---------->"+t);
//				String[] arrs=t.split(",");
//				List<Tuple2<String, Integer>> list=new ArrayList<Tuple2<String,Integer>>();
//				for(String arr: arrs) {
//					list.add(new Tuple2<String, Integer>(arr, 1));
//				}
//				return list.iterator();
//			}
//		});
		//过滤
		rddToPair=rddToPair.filter(new Function<Tuple2<Keys, User>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Keys, User> v1) throws Exception {
				Keys k=v1._1;
				System.out.println("filter-------->"+v1._1);
				if(k.getVideos() > 200 && k.getVideos() < 250) {
					//放行条件
					return true;	
				}
				//其他的拦截
				return false;
			}
		});
		//累加
//		rddToPair=rddToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
//			
//			@Override
//			public Integer call(Integer v1, Integer v2) throws Exception {
//			
//				return v1.intValue()+v2.intValue();
//			}
//		});
//		try {
//			testDeleteFile(args[1]);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//rddToPair.saveAsTextFile(args[1]);
		sc.stop();
		System.out.println("程序结束");
	}

}
