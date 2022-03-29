package ma.enset.tpmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class OccurencesReduce extends MapReduceBase
        implements Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
         int somme=0;
        while (values.hasNext()){
            somme+=values.next().get();
        }
        output.collect(key,new IntWritable(somme));
    }
}
