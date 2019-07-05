
package com.newroad.demo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.newroad.entity.Keys;
import com.newroad.entity.User;

import scala.Tuple2;

/**
 * 远程模式，将数字的字符串转换成int类型的集合map，遍历打印出来
 * @author Administrator
 *
 */
public class demo6_2{

	public static void main(String[] args) {
		
		String appName="spark程序名777";
		//本地模式
		String  master="spark://192.168.8.133:7077";
		master = "local";

		//创建spark配置文件															
		SparkConf sparkConf=new SparkConf().setAppName(appName).setMaster(master).set("num.executors", "2")
				.set("spark.default.parallelism", "50");
		//创建spark
		JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
		
		//JavaRDD<String> rdd=sparkContext.textFile(args[0]);		
		JavaRDD<String> rdd=sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\user.txt");
		
		//计算观看的top100  键值对的形式，键是观看视频数(Integer)和好友数，值是观看视频的user对象，string 是一行数据
		JavaPairRDD<Keys, User>  rddU = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Keys, User>() {

			//t 多行数据
			@Override
			public Iterator<Tuple2<Keys, User>> call(Iterator<String> t) throws Exception {
				int count = 0;
				List<Tuple2<Keys, User>> list = new ArrayList<>();
				User user = new User();
				Keys keys = new Keys();
				while(t.hasNext()) {
					count++ ;
					//以“ ”分割
					StringTokenizer st = new StringTokenizer(t.next());
					//设置单行数据
					while(st.hasMoreTokens()) {
						user.setUploader(st.nextToken());
						user.setVideos(Integer.valueOf(st.nextToken()));
						user.setFriends(Integer.valueOf(st.nextToken()));
						//设置过滤条件
						keys.setVideos(user.getVideos());
						keys.setFriends(user.getFriends());
					}
					
					list.add(new Tuple2<Keys, User>(keys, user));
				}
				System.out.println("count==========" + count);
				return list.iterator();
			}
			
			
		});
		
		//过滤
		rddU = rddU.filter(new Function<Tuple2<Keys,User>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<Keys, User> v1) throws Exception {
				Keys keys = v1._1;
				if(keys.getVideos() > 200 && keys.getVideos() < 250) {
					//放行条件
					return true;	
				}
				//其他的拦截
				return false;
			}
		});
		
		//根据好友数和视频数排序，从大到小
		rddU = rddU.sortByKey(new ComparatorChild1(), false);
		//取100条数据
		List<Tuple2<Keys, User>>  users = rddU.take(100);	
		//遍历取到的数据
		for(Tuple2<Keys, User> user : users) {
			System.out.println(user._1+" -----------> "+user._2.toString());
		}
		
		//rddU.saveAsTextFile(args[1]);  
		//rddU.saveAsTextFile("D:\\Users1.txt");
		
		//关闭，释放资源
		sparkContext.close();
	}
	
}

	/**
	 * 重写比较器：根据好友数从大到小排序，若相等则根据视频数从大到小排序
	 * @author Administrator
	 *
	 */
	class ComparatorChild1  implements Comparator<Keys>,Serializable {

		@Override
		public int compare(Keys o1, Keys o2) {
			if(o1.getFriends() > o2.getFriends()) {
				return 1;
			}
			
			if(o1.getFriends().equals(o2.getFriends())) {
				if(o1.getVideos() > o2.getVideos()) {
					return 1;
				}
				if(o1.getVideos().equals(o2.getVideos())) {
					return 0;
				}
				return -1;
			}
			
			return -1;
		}

	}
