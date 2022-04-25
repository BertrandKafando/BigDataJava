package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Iterator;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("Spark streaming HDFS").setMaster("local[*]");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.minutes(1));
        JavaDStream<String> lines= jsc.textFileStream("hdfs://localhost:9000/temp2/");

        JavaPairDStream<String,Integer>data=lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String tab[]=s.split(",");
                return new Tuple2<>(tab[2],Integer.parseInt(tab[3]));
            }
        });
        JavaPairDStream<String,Iterable<Integer> > dsmax=data.groupByKey();

        JavaPairDStream<String,Double>tempmean=dsmax.mapValues((integers -> {
            Iterator<Integer> it=integers.iterator();
            int i=0;
            double somme =0;
            while (it.hasNext()){
                somme=somme+it.next();
                i++;
            }
            double nbr=i;
            return  (somme/nbr);
        }));

        tempmean.print();


        jsc.start();
        jsc.awaitTermination();
    }
        }
