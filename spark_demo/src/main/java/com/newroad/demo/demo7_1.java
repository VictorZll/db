
package com.newroad.demo;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.newroad.entity.User;

import scala.Tuple2;

/**
 * 远程模式，将数字的字符串转换成int类型的集合map，遍历打印出来
 * @author Administrator
 *
 */
public class demo7_1{

	public static void main(String[] args) {
		
		/*if(args.length < 1) {
			System.out.println("error! need two params !!!");
			return;
		}*/
		
		String appName="spark程序名777";
		//本地模式
		String  master="spark://192.168.8.133:7077";
		master = "local";
		
		//System.out.println("----->"+Arrays.toString(args));
		
		//创建spark配置文件															
		SparkConf sparkConf=new SparkConf().setAppName(appName).setMaster(master).set("num.executors", "2");
		//创建spark
		JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
		
		JavaRDD<String> rdd=sparkContext.textFile(args[0]);		
		//JavaRDD<String> rdd=sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\user.txt");
		
		/*JavaRDD<User> rddU = rdd.map(new Function<String, User>() {

			@Override
			public User call(String v1) throws Exception {
				StringTokenizer  st = new StringTokenizer(v1);
				User user = new User();
				if(st.hasMoreTokens()) {
					user.setUploader(st.nextToken());
					user.setVideos(Integer.valueOf(st.nextToken()));
					user.setFriends(Integer.valueOf(st.nextToken()));
				}
							
				return user;
			}
			
		});*/
		
		//计算观看的top10  键值对的形式，键是观看视频数(Integer)，值是观看视频的user对象
		JavaPairRDD<Integer, User>  rddU = rdd.mapToPair(new PairFunction<String, Integer, User>() {

			@Override
			public Tuple2<Integer, User> call(String t) throws Exception {
				StringTokenizer  st = new StringTokenizer(t);
				User user = new User();
				if(st.hasMoreTokens()) {
					user.setUploader(st.nextToken());
					user.setVideos(Integer.valueOf(st.nextToken()));
					user.setFriends(Integer.valueOf(st.nextToken()));
				}
				return new Tuple2<Integer, User>(user.getVideos(), user);
				
			}

		});
		
		rddU = rddU.sortByKey(false);
		List<Tuple2<Integer, User>>  users = rddU.take(10);
		
		
		for(Tuple2<Integer, User> user : users) {
			System.out.println(user._1+" -----------> "+user._2);
		}
		
		//rddU.saveAsTextFile(args[1]);  
		rddU.saveAsTextFile(args[1]);
		
		//关闭，释放资源
		sparkContext.close();
	}
	
}
