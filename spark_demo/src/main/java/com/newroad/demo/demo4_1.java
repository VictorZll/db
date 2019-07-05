
package com.newroad.demo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
/**
 * 远程模式，将数字的字符串转换成int类型的集合map，遍历打印出来
 * @author Administrator
 *
 */
public class demo4_1 {

	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println("error! need two params !!!");
			return;
		}
		
		String appName="spark程序名demo4_1";
		//本地模式
		String  master="spark://192.168.8.133:7077";
		//master = "local";
		
		System.out.println("----->"+Arrays.toString(args));
		
		//创建spark配置文件															
		SparkConf sparkConf=new SparkConf().setAppName(appName).setMaster(master).set("num.executors", "2");
		//创建spark
		JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
		
		JavaRDD<String> rdd=sparkContext.textFile(args[0]);
		
		rdd = rdd.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				//以逗号分割
				String[]  arr = t.split(",");
				List<String>  list = Arrays.asList(arr);	
				for(String a:list) {
					System.out.println("a============" + a);
				}
				return list.iterator();
			}			
			
		});
			
		String sum = rdd.reduce(new Function2<String, String, String>() {

			@Override
			public String call(String v1, String v2) throws Exception {
				int sum = Integer.valueOf(v1)+Integer.valueOf(v2);
				System.out.println("sum======"+sum);
				return sum+"";
			}
			
		});
		
		rdd.saveAsTextFile(args[1]);
	
		//关闭，释放资源
		sparkContext.close();
	}
	
}
