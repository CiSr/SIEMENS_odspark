import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{unix_timestamp, to_date}
import org.apache.spark.sql.types.{StringType,StructType,DoubleType,StructField}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import scala.collection.mutable.ListBuffer

//Store the list of filenames in the Directory. 
val x=new java.io.File("/home/cibesridharan/Pictures/spark-1.6.1/bin/DFRead").listFiles.filter(_.getName.endsWith(".csv"))
//Each file has 4 features
val schemaString="Date,Time,Freq,Value"

//Associate DataTypes for it
val schema=schemaString.split(",").map{
field=>
if(field=="Value"||field=="Freq")
StructField(field,DoubleType)
else	
StructField(field,StringType)
}

val schema_applied=StructType(schema)
var fru = new ListBuffer[Double]()
val i=0
for(i<-0 until x.length){
val temp=x(i).toString
val df = sqlContext.read.format("com.databricks.spark.csv").
                         option("delimiter",",").
                         option("header","true"). 
                         schema(schema_applied).                         
                         load(temp)

val df_data_revised = df.select((to_date(unix_timestamp(
  $"Date", "dd.MM.yyyy"
).cast("timestamp"))).alias("Date"),df("Time"),df("Freq"),df("Value"))
//Group by Date on Value
val df_value_mean=df_data_revised.groupBy("Date").mean("Value").withColumnRenamed("avg(Value)","Value")
//Convert the date to numeric 
val df_numeric_lr=df_value_mean.select(unix_timestamp($"Date").alias("Date"),df_value_mean("Value"))

//COnvert into features, labels format
val featureCols = Array("Date","Value")
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val df2 = assembler.transform(df_numeric_lr)
val labelIndexer = new StringIndexer().setInputCol("Value").setOutputCol("label")
val df3 = labelIndexer.fit(df2).transform(df2)
//Build the regression model
val lr = new LinearRegression()
.setMaxIter(10)
.setRegParam(0.3)
.setElasticNetParam(0.8)
// Fit the model
val lrModel = lr.fit(df3)
val a=lrModel.coefficients(1)
val b=lrModel.coefficients(0)
val ddf=df_numeric_lr.select(abs($"Value"-$"Date"*a + b))
val temp1=ddf.withColumnRenamed(ddf.columns(0),"Deviation")
temp1.show
val t=temp1.sort(desc("Deviation")).take(2)
val z=t.map(_.getDouble(0)).toList
fru+=z(0)
fru+=z(1)
}



