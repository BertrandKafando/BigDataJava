import ma.enset.OccurencesReducer;
import ma.enset.OccurencesReducer2;
import ma.enset.OcurrencesMapper;
import ma.enset.OcurrencesMapper2;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application {
    public static void main(String[] args) throws IOException {

        JobConf conf=new JobConf();
        conf.setJobName("Nomre de mots");
        conf.setJarByClass(Application.class);

        conf.setMapperClass(OcurrencesMapper2.class);
        conf.setReducerClass(OccurencesReducer2.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));


/*
        FileInputFormat.addInputPath(conf,new Path("test.csv"));
        FileOutputFormat.setOutputPath(conf,new Path("./output"));


 */

        JobClient.runJob(conf);
        //hadoop jar tpmap-1-0-SNAPSHOT.jar ma.enset.tpmap.Application /vente.txt /
        //Not a valid JAR: /tpma
    }
}
