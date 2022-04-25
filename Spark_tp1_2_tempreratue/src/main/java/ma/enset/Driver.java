package ma.enset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;

public class Driver {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("temperature").setMaster("local[*]");
        JavaSparkContext  context=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=context.textFile("1750.csv");

        //temp moyenne minimale
        JavaPairRDD<String,Integer>rdd2=rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String tab[]=s.split(",");
               return new Tuple2<>(tab[2],Integer.parseInt(tab[3]));
            }
        });


        JavaPairRDD<String, Iterable<Integer>> rdd3=rdd2.groupByKey();
        rdd3.foreach(p-> System.out.println(p));
    }




}
