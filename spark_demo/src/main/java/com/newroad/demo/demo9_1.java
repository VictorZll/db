package com.newroad.demo;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.newroad.entity.User;

import scala.Tuple2;
/**
 *  spark_session 远程访问文件在本地运算
 * @author Administrator
 *
 */
public class demo9_1 {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序demo9";
		String master="local";
		String file_in_path=args[0];
		//String master="spark://192.168.8.133:7077";
		
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("num.executors", "4");
		conf.set("executor-cores", "2");
		conf.set("executor-memory", "500M");
		conf.set("spark.default.parallelism", "5");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> rdd=sc.textFile(file_in_path);
		
		 // 读取一行的数据：并且转换为User对象
		JavaRDD<User>  rdd2 = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, User>() {

			@Override
			public Iterator<User> call(Iterator<String> t) throws Exception {
				List<User> list =new ArrayList<User>();
				while(t.hasNext()) {
					String  tt = t.next();
					StringTokenizer v = new StringTokenizer(tt);
					User user = new User();
					if(v.hasMoreTokens()) {
						user.setUploader(v.nextToken());
					}
					if(v.hasMoreTokens()) {
						user.setVideos(Integer.parseInt(v.nextToken()));
					}	
					if(v.hasMoreTokens()) {
						user.setFriends(Integer.parseInt(v.nextToken()));
					}		
					list.add(user);
				}
				return list.iterator();
			}
		});
		
		SparkSession sks=SparkSession.builder().master(master).appName(appName).config("spark.some.config.option", "some-value").getOrCreate();
		Dataset<Row> df=sks.createDataFrame(rdd2, User.class);
		df.createOrReplaceTempView("spark_tb_1");//临时表
		df = sks.sql("select * from spark_tb_1 "
				+ "where (friends > 20 and friends <100) "
				+ "and (videos > 320 and videos <331) "
				+ "order by videos DESC,friends DESC "
				+ "limit 200");
		df.show(208);
		sc.close();
		sks.close();
		

	}

}
