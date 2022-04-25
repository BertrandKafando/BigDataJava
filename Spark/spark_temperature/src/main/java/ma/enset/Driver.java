package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

public class Driver {
    //Exercice 3
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("temperature").setMaster("local[*]");
        JavaSparkContext context=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=context.textFile("1750.csv");

        //temp moyenne minimale
        JavaPairRDD<String,Integer> rdd2=rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String tab[]=s.split(",");
                return new Tuple2<>(tab[2],Integer.parseInt(tab[3]));
            }
        });


        JavaPairRDD<String, Iterable<Integer>> rddmin=rdd2.groupByKey().filter((key)->(key._1().equals("TMIN")));
        JavaPairRDD<String, Iterable<Integer>> rddmax=rdd2.groupByKey().filter((key)->(key._1().equals("TMAX")));
        /*
        * je peux filtrer ici .filter((key)->(key._1().equals("TMIN")) et TMAX
         * */

        JavaPairRDD<String, Double>rrdmeanmin=rddmin.mapValues(new Function<Iterable<Integer>, Double>() {
            @Override
            public Double call(Iterable<Integer> integers) throws Exception {
              Iterator<Integer>it=integers.iterator();
              int i=0;
              double somme =0;
              while (it.hasNext()){
                  somme=somme+it.next();
                  i++;
              }
              double nbr=i;
                return  (somme/nbr);
            }
        });
       // rddmin.foreach(p-> System.out.println(p));
        //rrd.collect().forEach(p-> System.out.println(p));

        //temp max + eleve
        JavaPairRDD<String, Double>rrdmaxmax=rddmax.mapValues(new Function<Iterable<Integer>, Double>() {
            @Override
            public Double call(Iterable<Integer> integers) throws Exception {
                Iterator<Integer>it=integers.iterator();
                int i=0;
                double max=0;
                while (it.hasNext()){
                   if(max<it.next()) max= it.next();
                }
                return  max;
            }
        });
        //rrdmaxmax.collect().forEach(p-> System.out.println(p));



        //top 5 station chaudes
        JavaPairRDD<String, Iterator> rddmax1= rddmax.flatMapValues(new FlatMapFunction<Iterable<Integer>, Iterator >() {
            @Override
            public Iterator call(Iterable<Integer> integers) throws Exception {
                Iterator it=integers.iterator();
                List<Integer>list=new ArrayList<>();
                while(it.hasNext()){
                    list.add((Integer) it.next());
                }
                Collections.sort(list);
                return list.iterator();
            }

        });
        rddmax1.foreach(p-> System.out.println(p));


    }
}
