package ma.enset.Dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.col;

public class ApplicationCsv {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("tp spark").
                master("local[*]").getOrCreate();

        Dataset<Row> df=ss.read().option("header",true).csv("employe.csv");
        //df.show();

        //df.printSchema();
        //age 30 38

        Dataset<Row> ageintervalle=  df.filter("age>=30 and age<=38");
        //ageintervalle.show();



        Dataset<Row> salarymean=df.select(col("departement"),col("salary").cast("double")).groupBy(col("departement")).avg((Seq<String>) col("salary"));
      //  salarymean.show();


        //nombre de salaries par dep
        Dataset<Row> nbreSal=df.groupBy(col("departement")).count();
        //nbreSal.show();


        //salaire max/departement
        Dataset<Row>  salarymaxbydep=df.groupBy(col("departement")).max(String.valueOf(col("salary").cast("double")));
        salarymaxbydep.show();



    }
}
