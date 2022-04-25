package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application1 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf =new SparkConf().setAppName("sparkStreaming").setMaster("local[2]");
        JavaStreamingContext jsp= new JavaStreamingContext(conf,Durations.seconds(10));
        JavaReceiverInputDStream<String>lines=jsp.socketTextStream("localhost",9090);
        JavaDStream<String>words=lines.flatMap(s-> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String,Integer>woordPair=words.mapToPair(s->new Tuple2<>(s,1));
        JavaPairDStream<String,Integer>wordcount=woordPair.reduceByKey((v1,v2)->v1+v2);
        wordcount.print();


        jsp.start();
        jsp.awaitTermination();

                //nc -lk 9090
    }
}
