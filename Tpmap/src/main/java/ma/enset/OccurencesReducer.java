package ma.enset;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class OccurencesReducer extends MapReduceBase
        implements Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int val,max=0,min=0;
        if(values.hasNext()){
            min=max=values.next().get();
        }
        while (values.hasNext()){
            val=values.next().get();
            if(val >max) max=val;
            if(val < min) min=val;
        }

        output.collect(new Text("salaire max du depart : "+key.toString()),new IntWritable(max) );
        output.collect(new Text("salaire min du depart : "+key.toString()),new IntWritable(min) );
    }
}