package ma.enset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Iterator;

public class Driver2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("temperature").setMaster("local[*]");
        JavaSparkContext context=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=context.textFile("1750.csv");
        //temp moyenne minimale
        //envoie le nom de la data (tmax ou tmin) et la valeur
        JavaPairRDD<String,Integer> rdd2=rdd1.mapToPair((s -> {
            String tab[]=s.split(",");
            return new Tuple2<>(tab[2],Integer.parseInt(tab[3]));
        })).persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Iterable<Integer>> rddmin=rdd2.groupByKey().filter((key)->(key._1().contains("TMIN")));
        JavaPairRDD<String, Iterable<Integer>> rddmax=rdd2.groupByKey().filter((key)->(key._1().contains("TMAX")));
        //temp max + eleve
        JavaPairRDD<String, Double>rrdmaxmax=rddmax.mapValues((integers -> {
            Iterator<Integer> it=integers.iterator();
            double max=0;
            while (it.hasNext()){
                if(max<it.next()) max= it.next();
            }
            return  max;
        }));
       // rddmax.collect().forEach(p-> System.out.println(p));
       // rrdmaxmax.collect().forEach(p-> System.out.println(p));

        //temp min  - eleve
        JavaPairRDD<String, Double>rrdminmin=rddmax.mapValues((integers -> {
            Iterator<Integer> it=integers.iterator();
            double min=100000;
            while (it.hasNext()){
                if(min>it.next()) min= it.next();
            }
            return  min;
        }));

        rddmin.collect().forEach(p-> System.out.println(p));
        rrdminmin.collect().forEach(p-> System.out.println(p));
    }
}
