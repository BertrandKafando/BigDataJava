package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class KmeansDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {


        long counter;


        int iteration=0;
        Path file=new Path("/input/data.txt");

        while (true) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Kmeans job");


            job.setJarByClass(KmeansDriver.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);


            job.addCacheFile(new URI("hdfs://localhost:9000/input/center.txt"));

            FileInputFormat.addInputPath(job, file);
            FileOutputFormat.setOutputPath(job, new Path("/output/oup"+iteration));

            job.waitForCompletion(true);

            counter=job.getCounters().findCounter(Counter.CONVERGED).getValue();
            System.out.println("////////////////////////////////////////////////////////////////counter "+counter);
            if(iteration>=10){

                break;
            }
            iteration++;

        }
    }

}
