
package com.newroad.demo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.newroad.entity.User;
/**
 * 复制的代码
 * @author Administrator
 *
 */
public class demo9_2 {
	
	static String hdfs_input = "hdfs://zll:9000/user.txt";
	static String appName = "spark sql 01";
	static String master = "local";
	
	public static void main(String[] args) {
		
		//创建spark配置信息
		SparkConf conf =  new SparkConf().setAppName(appName);
		conf.setMaster(master);
		conf.set("spark.ui.killEnabled", "false");
		conf.set("spark.executor.memory", "512M");
		conf.set("num.executors", "2");
		//创建spark
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = sc.textFile(hdfs_input);
		
		JavaRDD<User> rdd1 = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, User>() {

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
		
		SparkSession spark = SparkSession
		 .builder()
	     .master(master)
	     .appName("Word Count")
	     .config("spark.some.config.option", "some-value")
	     .getOrCreate();
		
		// rdd1 = JavaRDD<User>                      User 对象
		Dataset<Row> df = spark.createDataFrame(rdd1,User.class);
		df.createOrReplaceTempView("User_sprak");
		df = spark.sql("select * from User_sprak "
				+ "where (friends > 20 and friends <100) "
				+ "and (videos > 320 and videos <331) "
				+ "order by videos DESC,friends DESC "
				+ "limit 200");
		
		 //错df.join(df1, "friends", "videos")
		df.show(201);
		sc.stop();
		spark.stop();
		
	}

}
