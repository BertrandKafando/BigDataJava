package ma.enset;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.jetty.servlet.Context;

import java.io.IOException;
import java.util.Iterator;

public class KmeansReducer extends Reducer<Text,Text,Text,Text> {
   /* @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double somme=0;
        int nb_points=0;
        Iterator<Text> it=values.iterator();
        while (it.hasNext()){
            somme+=it.next().get();
            nb_points++;
        }
        double mean=somme/nb_points;
        context.write(key,new DoubleWritable(mean));

    }*/

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
       double sommeX=0,sommeY=0,meanX,meanY;
       int nb_points=0;
        Iterator<Text> it=values.iterator();
        while (it.hasNext()){
            String pts[]=it.next().toString().split(",");
            sommeX+=Double.parseDouble(pts[0]);
            sommeY+=Double.parseDouble(pts[1]);
            nb_points++;
        }
        meanX=sommeX/nb_points; meanY=sommeY/nb_points;
        context.write(key,new Text(new Point(meanX,meanY).toString()));
    }
}
