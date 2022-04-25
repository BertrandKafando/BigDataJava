package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
import java.util.function.ToDoubleFunction;

public class Driver3 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("temperature").setMaster("local[*]");
        JavaSparkContext context=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=context.textFile("1750.csv");
        //temp moyenne minimale
        //envoie le nom de la data (tmax ou tmin) et la valeur
        JavaPairRDD<String,Integer> rdd2=rdd1.mapToPair((s -> {
            String tab[]=s.split(",");
            return new Tuple2<>(tab[2]+"_"+tab[0],Integer.parseInt(tab[3]));
        })).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String, Iterable<Integer>> rddmin=rdd2.groupByKey().filter((key)->(key._1().contains("TMIN"))).persist(StorageLevel.MEMORY_ONLY()); //filtre ceux qui contiennent les min
        JavaPairRDD<String, Iterable<Integer>> rddmax=rdd2.groupByKey().filter((key)->(key._1().contains("TMAX"))).persist(StorageLevel.MEMORY_ONLY());
        //tminmean pour chaque station
        JavaPairRDD<String, Double> rrdminmin=rddmin.flatMapValues((integers -> {
            int i=0;
            List<Double> list=new ArrayList<>();
            Iterator<Integer>it=integers.iterator();
            double somme=0;
            while (it.hasNext()){
                somme=somme+it.next();
                i++;
            }
            list.add((somme/i));
            return list.iterator();
        })) ;
        //sorting du liste de min
        List<Tuple2<String,Double>> list=new ArrayList<>();
        rrdminmin.collect().forEach(p->{
            list.add(p);
        });
        Collections.sort(list, Comparator.comparingDouble((t->{return t._2();})));
        //les plus froides
        System.out.println("Top 5 les plus froides");
        for(int i=0;i<5;i++){
            System.out.println(list.get(i));
        }


        //tmaxmean
        JavaPairRDD<String, Double>  rrdmaxmax=rddmax.flatMapValues((integers -> {
            int i=0;
            List<Double> list2=new ArrayList<>();
            Iterator<Integer>it=integers.iterator();
            double somme=0;
            while (it.hasNext()){
                somme=somme+it.next();
                i++;}
            list2.add((somme/i));
            return list2.iterator();
        }));
        List<Tuple2<String,Double>> list_chaudes=new ArrayList<>();
        rrdmaxmax.collect().forEach(p->{
            list_chaudes.add(p);
        });
        Collections.sort(list_chaudes, Comparator.comparingDouble((t->{return t._2();})));
        //affichage plus chaudes
        System.out.println("Top 5 les plus chaudes");
        for(int i=list_chaudes.size()-1;i>list_chaudes.size()-6;i--){
            System.out.println(list_chaudes.get(i));
        }


    }



}
