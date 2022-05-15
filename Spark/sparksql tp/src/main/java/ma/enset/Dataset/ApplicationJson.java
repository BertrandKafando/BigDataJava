package ma.enset.Dataset;

import ma.enset.Model.Employe;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ApplicationJson {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("tp spark").
                master("local[*]").getOrCreate();

        Encoder<Employe>employeEncoder= Encoders.bean(Employe.class);

        //dif
        Dataset<Employe> ds=ss.read().option("multiLine",true).json("employe.json").as(employeEncoder);
        ds.filter((FilterFunction<Employe>) emp->emp.getAge()>=30 && emp.getAge()<=35).show();

    }
}
