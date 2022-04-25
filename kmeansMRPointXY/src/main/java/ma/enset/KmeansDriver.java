package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class KmeansDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {




        int iteration=0;
        Path file=new Path("/input/datav2.txt");

        while (true) {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Kmeans job");


            job.setJarByClass(KmeansDriver.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);


            job.addCacheFile(new URI("hdfs://localhost:9000/input/centerv2.txt"));

            FileInputFormat.addInputPath(job, file);
            FileOutputFormat.setOutputPath(job, new Path("/output/oup"+iteration));

            job.waitForCompletion(true);


            FileSystem fs=FileSystem.get(conf);
            // replace centroids with new centroids from last output file after the end of every job //
            FSDataOutputStream out = fs.create(new Path("hdfs://localhost:9000/input/centerv2.txt"), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
            // get new centroids from last output
            InputStreamReader is = new InputStreamReader(fs.open(new Path("hdfs://localhost:9000/output/oup" + iteration + "/part-r-00000")));
            BufferedReader br = new BufferedReader(is);
            String line = null;
            StringBuilder old_centroid = new StringBuilder();
            StringBuilder new_centroid = new StringBuilder();
            while ((line = br.readLine()) != null) {
                String part[]= line.split("__");
                //new centroids
                new_centroid.append(part[1]);
                new_centroid.append("\n");
                //old centroids
                old_centroid.append(part[0]);
                old_centroid.append("\n");
            }
            // if old centroids == new centroids or iterations>=10 -> end while
            //new_centroid.toString().equals(old_centroid.toString())
            System.out.println(old_centroid.toString());
            System.out.println(new_centroid.toString());
            //don't forget to delete space
            if((new_centroid.toString().replaceAll("\\s+","")).equals((old_centroid.toString().replaceAll("\\s+","")))|| iteration>=10){
                System.out.println("stop");
                break;
            }
            // save new centroids to centerMRI.txt
            bw.write(new_centroid.toString());
            bw.close();
            br.close();
            iteration++;

        }
    }

}
