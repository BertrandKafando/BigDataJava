package ma.enset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class OcurrencesMapper2 extends MapReduceBase
        implements Mapper<LongWritable, Text,Text, Text> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      /*  String ventes[]=value.toString().split("");

            output.collect(new Text(ventes[0]+"-"+ventes[1]),new IntWritable(Integer.parseInt(ventes[3])));*/
        String ent[]=value.toString().split(",");

        output.collect(new Text(ent[2]),new Text(ent[0]+"-"+ent[1]));

    }
}
