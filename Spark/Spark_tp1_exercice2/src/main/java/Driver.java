import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Driver {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("ventes").setMaster("local[*]");
        JavaSparkContext  context=new JavaSparkContext(conf);
        JavaRDD<String>rdd1=context.textFile("data.txt");
        //JavaRDD<String>rdd2=rdd1.flatMap(s-> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String,Integer>rdd3=rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[]tab=s.split(" ");
                return new Tuple2<>(tab[1],Integer.parseInt(tab[3]));  //(key=ville, value=prix de vente)
            }
        });
        JavaPairRDD<String,Integer>rddgroup=rdd3.reduceByKey((v1,v2)->v1+v2);  //sommation des prix
            //calcul
        rddgroup.foreach(p-> System.out.println(p));


    }
}
