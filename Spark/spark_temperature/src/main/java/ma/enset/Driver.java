package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class Driver {
    //Exercice 3
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("temperature").setMaster("local[*]");
        JavaSparkContext context=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=context.textFile("2020.csv");
        //temp moyenne minimale
        //envoie le nom de la data (tmax ou tmin) et la valeur
        JavaPairRDD<String,Integer> rdd2=rdd1.mapToPair((s -> {
            String tab[]=s.split(",");
            return new Tuple2<>(tab[2],Integer.parseInt(tab[3]));
        })).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String, Iterable<Integer>> rddmin=rdd2.groupByKey().filter((key)->(key._1().equals("TMIN"))).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String, Iterable<Integer>> rddmax=rdd2.groupByKey().filter((key)->(key._1().equals("TMAX"))).persist(StorageLevel.MEMORY_ONLY());
        /*
        * avec grouby je regroupe les donnees de chaque key(tmax,tmin,...) et je stocke dans dans les tuples (key,[liste _value])
        * je peux filtrer ici .filter((key)->(key._1().equals("TMIN")) et TMAX
        *
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
        //calcul de la moyenne
        JavaPairRDD<String, Double>rrdmeanmax=rddmax.mapValues((integers -> {
            int i=0;
            Iterator<Integer>it=integers.iterator();
            double somme=0;
            while (it.hasNext()){
                somme=somme+it.next();
                i++;
            }
            return  (somme/i);
        }));
         rddmin.collect().forEach(p-> System.out.println(p));
        rrdmeanmin.collect().forEach(p-> System.out.println(p));






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
       // rddmax1.foreach(p-> System.out.println(p));


    }
}
