import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Driver {
    public static void main(String[] args) {
        SparkConf  conf=new SparkConf().setAppName("liste").setMaster("local[*]");
        JavaSparkContext cont =new JavaSparkContext(conf);
        //Liste
        List<String> liste=new ArrayList<>(); liste.add("Bertrand");
        liste.addAll(Arrays.asList("younes","Faoud","yasmine","omar","Bertrand","omar","Cecile","yao","12","30","80","90","100"));
        //Parallelize
        JavaRDD<String> rdd1=cont.parallelize(liste);
        //flatMap
        JavaRDD<String> rdd2=rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.toUpperCase()).iterator();
            }
        });

       //filter1 length>6
        JavaRDD<String>rdd3=rdd2.filter(s->(s.length()>6&& !s.chars().allMatch(Character::isDigit)));
        //filter length<6
        JavaRDD<String>rdd4=rdd2.filter(s->(s.length()<6&& !s.chars().allMatch(Character::isDigit)));
        //filter digit
        JavaRDD<String>rdd5=rdd2.filter(s->(s.chars().allMatch(Character::isDigit)));


        //union pour refaire une liste avec tous les noms sans les chiffres
        JavaRDD<String>rdd6=rdd3.union(rdd4);

        //mapper pair
        JavaPairRDD<String,Integer>rdd71=rdd5.mapToPair(c->new Tuple2<>(c,1));
        //mapper la liste et les transformer en lowercase et mappair
        JavaPairRDD<String,Integer>rdd81=rdd6.mapToPair(s->new Tuple2<>(s.toLowerCase(),1));

        //reduceBy key
        JavaPairRDD<String,Integer>rdd8=rdd81.reduceByKey((v1,v2)->v1+v2);
        JavaPairRDD<String,Integer>rdd7=rdd71.reduceByKey((v1,v2)->v1+v2);

        //union
        JavaPairRDD<String,Integer>rdd9=rdd8.union(rdd7);

        //sortBy
        JavaPairRDD<String,Integer>rdd10=rdd9.sortByKey(true);
        rdd10.collect();
        rdd10.foreach(p-> System.out.println(p));
    }
}
