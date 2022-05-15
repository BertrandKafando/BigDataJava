package ma.enset;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.util.Arrays;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;



public class Application {


    public static void main(String[] args) throws InterruptedException {


        Logger.getLogger("org").setLevel(Level.OFF);

        System.setProperty("twitter4j.oauth.consumerKey","tzP2Hm8tndSVa7lIuceo4Etx8");
        System.setProperty("twitter4j.oauth.consumerSecret", "qHRIZzgZUdbPJlSlswOw8WupfL9z5hY9Kq48JKB8asIe76nWCw");
        System.setProperty("twitter4j.oauth.accessToken", "1205941941776650241-qUIKA4UCWhLGIU7DgdzIm26QZvW7Xm");
        System.setProperty("twitter4j.oauth.accessTokenSecret","Wc6yYyQIy7uDjhQ4eTcWpveyjPplskX14TXeHSjfxeYvW");
        SparkConf conf=new SparkConf().setAppName("Spark streaming HDFS").setMaster("local[2]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.minutes(100));


        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);


        // Without filter: Output text of all tweets
        JavaDStream<String> statuses = twitterStream.map(new Function<Status, String>() {
            @Override
            public String call(Status status) throws Exception {
                return status.getText();
            }
        });
      //  public String call(Status status) { return status.getText(); }
        statuses.print();
        jssc.start();
        statuses.print();
        jssc.start();


    }
}
