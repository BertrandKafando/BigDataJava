package ma.enset.Dataset;

import ma.enset.Model.Employe;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class ApplicationJson {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("tp spark").
                master("local[*]").getOrCreate();

        Encoder<Employe>employeEncoder= Encoders.bean(Employe.class);

        //dif
        Dataset<Employe> ds=ss.read().option("multiLine",true).json("employe.json").as(employeEncoder);

        Dataset<Employe> em=ds.filter((FilterFunction<Employe>) emp->emp.getAge()>=30 && emp.getAge()<=35);
        //em .show();

        //mean
        Dataset<Row> meandata=ds.groupBy(col("departement")).avg(String.valueOf(col("salary")));
       meandata.show();

        //nombre de salaries par dep
        Dataset<Row> nbreSal=ds.groupBy(col("departement")).count();
        //nbreSal.show();


        //salaire max/departement
        //le max des max select(max("col"))
        Dataset<Row>  salarymaxbydep=ds.groupBy(col("departement")).max("salary");
        //salarymaxbydep.show();

    }
}
