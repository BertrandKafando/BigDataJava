package ma.enset.tpmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class OccurencesMapper extends MapReduceBase
        implements Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String prenom[]=value.toString().split(" ");
        for (String p:prenom) {
            output.collect(new Text(p),new IntWritable(1));
        }
    }
}
