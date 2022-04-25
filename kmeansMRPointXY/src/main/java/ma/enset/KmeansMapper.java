package ma.enset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KmeansMapper extends Mapper<LongWritable, Text, Text,Text> {
   // List<Double>centers=new ArrayList<>();
    List<Centroid>centres=new ArrayList<>();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text ,Text>.Context context) throws IOException, InterruptedException {
        //centers.clear();
        centres.clear();
        URI[] uri= context.getCacheFiles();
        FileSystem fs=FileSystem.get(context.getConfiguration());
        //InputStreamReader is=new InputStreamReader(fs.open(new Path(uri[0])));
        BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(uri[0]))));
        String ligne="";
        while((ligne=reader.readLine())!=null){
            String tab[]=ligne.split(",");
           // double centre=Math.sqrt( Math.pow(Double.parseDouble(tab[0]),2)+Math.pow(Double.parseDouble(tab[1]),2));
            Centroid centroid=new Centroid(Double.parseDouble(tab[0]),Double.parseDouble(tab[1]));
            centres.add(centroid);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

      //  double p=Double.parseDouble(value.toString());
        double min=Double.MAX_VALUE,d;
              Centroid  nearest_center=new Centroid();
        String tab[]=value.toString().split(",");

        Point point=new Point(Double.parseDouble(tab[0]),Double.parseDouble(tab[1]));

        for (Centroid c:centres
             ) {
            d=Math.sqrt(Math.pow(point.getX()-c.getX(),2)+Math.pow(point.getY()-c.getY(),2));
            if (d<min){
                min=d;
                nearest_center=c;
            }
        }
        nearest_center.getPoints().add(point);
         Point centre=new Point(nearest_center.getX(),nearest_center.getY());

        context.write(new Text(centre.toString()),value);

    }
}
