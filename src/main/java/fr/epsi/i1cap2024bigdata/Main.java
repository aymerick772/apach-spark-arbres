package fr.epsi.i1cap2024bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class Main {
    private static final String COMMA_DELIMITER = ";";
    public static void EtlUsingRDD(){

//        CREATE CONTEXT
    SparkConf conf = new SparkConf().setAppName("lesarbres").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
//    LOAD DATASETS
        JavaRDD<String> linesWithHeader = sparkContext.textFile("file:///C:/Users/aymer/Downloads/les-arbres.csv");

//        TRANSFORMATIONS
//        exlude headers line
        JavaRDD<String> lines = linesWithHeader.filter(line->!line.startsWith("IDBADE"));
//        Map Line ti Object
        JavaRDD<String[]> arbres_data = lines.map(line->line.split(COMMA_DELIMITER));
// Sort Data by ARRONDISSEMENT
        JavaRDD<String[]> rddSorted = arbres_data.sortBy(f->f[3],true,1);
        rddSorted.collect().forEach(f -> System.out.println(f[0]+","+f[1]+","+f[2]+","+f[3]));
        // Close Context (To avoid memory leaks)
        sparkContext.close();
    }
    public static StructType minimumCustomerDataSchema() {
        return DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("type_emplacement", DataTypes.StringType, true),
                DataTypes.createStructField("domanialite", DataTypes.StringType, true),
                DataTypes.createStructField("arrondissement", DataTypes.StringType, true),
                DataTypes.createStructField("complement_adresse", DataTypes.StringType, true),
                DataTypes.createStructField("numero", DataTypes.StringType, true),
                DataTypes.createStructField("lieu_adresse", DataTypes.StringType, true),
                DataTypes.createStructField("id_emplacement", DataTypes.StringType, true),
                DataTypes.createStructField("libelle_francais", DataTypes.StringType, true),
                DataTypes.createStructField("genre", DataTypes.StringType, true),
                DataTypes.createStructField("espece", DataTypes.StringType, true),
                DataTypes.createStructField("variete_oucultivar", DataTypes.StringType, true),
                DataTypes.createStructField("circonference", DataTypes.DoubleType, true),
                DataTypes.createStructField("hauteur", DataTypes.DoubleType, true),
                DataTypes.createStructField("stade_developpement", DataTypes.StringType, true),
                DataTypes.createStructField("remarquable", DataTypes.StringType, true),
                DataTypes.createStructField("geo_point_2d", DataTypes.StringType, true)}
        );
    }
    public static void EtlUsingDataframe(){
        // Create a Spark Session
        SparkSession sparkSession = SparkSession.builder().appName("Customer Aggregation pipeline").master("local[3]").getOrCreate();
        //sparkSession.sparkContext().setLogLevel("WARN");

        // Get Data from Kaggle : https://www.kaggle.com/c/titanic/data
        String dataFile = "file:///C:/Users/aymer/Downloads/les-arbres.csv";

        // Goal : We will be predicting if a passenger survived or not depending on its features.

        // Collect data into Spark
        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", ";")
                .load(dataFile);

        // show 20 top results
        df.show();
//        df.printSchema();
//        // Additional informations
//        System.out.println("Nombre de lignes : "+df.count());
//        System.out.println("Colonnes : "+ Arrays.toString(df.columns()));
//        System.out.println("Types de données : "+Arrays.toString(df.dtypes()));
//
//
        Dataset<Row> df_grouped = df.groupBy("arrondissement").count();
        df_grouped.show();
//
        Map<String, String> test = new HashMap<String,String>();
        test.put("CIRCONFERENCE (cm)", "mean");
        test.put("HAUTEUR (m)", "mean");
        test.put("IDBASE","count");
        Dataset<Row> df_grouped_2 = df.groupBy("genre").agg(test);
        df_grouped_2.show();

        df_grouped_2.sort(df_grouped_2.col("avg(HAUTEUR (m))").desc()).show();

        df.createOrReplaceTempView("Tree");
        df.sqlContext().sql("SELECT 'LIBELLE FRANCAIS', COUNT(*) FROM Tree GROUP BY 'LIBELLE FRANCAIS' ORDER BY count(*) DESC").show();

        sparkSession.stop();

    }


    public static void main(String[] args) {

//        Logger.getLogger("org").setLevel(Level.ERROR);
//        EtlUsingRDD();
        EtlUsingDataframe();
    }
}