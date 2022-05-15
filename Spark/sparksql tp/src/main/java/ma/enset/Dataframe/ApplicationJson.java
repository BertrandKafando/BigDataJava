package ma.enset.Dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static  org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

public class ApplicationJson {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("tp spark").
                master("local[*]").getOrCreate();

        Dataset<Row> df=ss.read().option("multiLine",true).json("employe.json");
        //df.printSchema();
        //age 30 38

            //col("age").between(30,39);
        Dataset<Row> ageintervalle=  df.filter("age>=30 and age<=38");
        //ageintervalle.show();

        //mean
        Dataset<Row> salarymean=df.groupBy(col("departement")).avg(String.valueOf(col("salary")));
        salarymean.show();


        //nombre de salaries par dep
        Dataset<Row> nbreSal=df.groupBy(col("departement")).count();
        //nbreSal.show();


        //salaire max/departement
        //le max des max select(max("col"))
        Dataset<Row>  salarymaxbydep=df.groupBy(col("departement")).max("salary");
        //salarymaxbydep.show();


    }
}
