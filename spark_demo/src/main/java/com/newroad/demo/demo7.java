package com.newroad.demo;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.newroad.entity.Keys;
import com.newroad.entity.User;

import scala.Tuple2;
/**
 * 序列化--内部类
 * @author Administrator
 *
 */
public class demo7 {
	public static void main(String[] args) throws InterruptedException {
		String appName="我是spark启动的程序demo4";
		//String master="local";//本地连接
		String master="spark://zll:7077";//远程连接
		SparkConf conf=new SparkConf().setAppName(appName).setMaster(master);
		conf.set("num.executors", "2");
		conf.set("executor-cores", "2");
		conf.set("executor-memory", "600M");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> rdd=sc.textFile(args[0]);
		JavaPairRDD<Keys,User> rdd2=rdd.mapToPair(new PairFunction<String, Keys, User>() {

			@Override
			public Tuple2<Keys,User> call(String t) throws Exception {
				StringTokenizer st = new StringTokenizer(t);
				User u=new User();
				Keys k=new Keys();
				if(st.hasMoreTokens()) {
					u.setUploader(st.nextToken());
					u.setVideos(Integer.valueOf(st.nextToken()));
					u.setFriends(Integer.valueOf(st.nextToken()));
					k.setFriends(Integer.valueOf(u.getFriends()));
					k.setVideos(Integer.valueOf(u.getVideos()));
				}
				return new Tuple2<Keys, User>(k, u);
			}
		});
		/**
		 * 匿名内部类
		 */
		rdd2=rdd2.filter(new Function<Tuple2<Keys,User>, Boolean>() {
		
			@Override
			public Boolean call(Tuple2<Keys, User> v1) throws Exception {
				Keys keys=v1._1;
				if(keys.getVideos()>320&&keys.getVideos()<330) {
					if(keys.getFriends()>20&&keys.getFriends()<100) {
						return true;
					}
				}
				return false;
			}
		});
		rdd2=rdd2.sortByKey(new Comparator1(),false);//倒序
		List<Tuple2<Keys, User>> users=rdd2.take(50);
		for(Tuple2<Keys, User> user:users) {
			System.out.println(user._1+"---------------->"+user._2.toString());
		}
	
		//rdd2.saveAsTextFile(args[1]);

	
		sc.stop();
	}

}

 class Comparator1 implements Comparator<Keys>,Serializable{

	@Override
	public int compare(Keys o1, Keys o2) {
		if(o1.getVideos().equals(o2.getVideos())) {
			return o1.getFriends()-o2.getFriends();
		}else {
			return o1.getVideos()-o2.getVideos();
		}
	}

	
}
