package ma.enset;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class OccurencesReducer2 extends MapReduceBase
        implements Reducer<Text, Text,Text,IntWritable> {

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       int nbre=0;
        while ( values.hasNext()){
           nbre=nbre+1;
        }

        output.collect(new Text("nbre employés du depart : "+key.toString()),new IntWritable(nbre) );

    }
}