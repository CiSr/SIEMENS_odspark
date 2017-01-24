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
import org.apache.spark.rdd.RDD

//Gets  the list of filenames in the Directory. 
val x=new java.io.File("/home/cibesridharan/Pictures/spark-1.6.1/bin/DFRead").listFiles.filter(_.getName.endsWith(".csv"))

//------------------------------------------------------------//
//Each file has 4 features
//Associate Date and Time as String
//Freq Value as Double


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

//Declare the list to add maximum of two surprise values. 

var surprise_values_list = new ListBuffer[Double]()


val i=0
for(i<-0 until x.length){
val temp=x(i).toString
//Load file by file. 
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

//ml is used since we use a dataframe

//Convert into features, labels format
val featureCols = Array("Date","Value")
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val df2 = assembler.transform(df_numeric_lr)
val labelIndexer = new StringIndexer().setInputCol("Value").setOutputCol("label")
val training = labelIndexer.fit(df2).transform(df2)
//Build the regression model
val lr = new LinearRegression()
.setMaxIter(10)
.setRegParam(0.3)
.setElasticNetParam(0.8)
// Fit the model

val lrModel = lr.fit(training)
//Get the coefficients. 
val a=lrModel.coefficients(1)
val b=lrModel.coefficients(0)
//
val ddf=df_numeric_lr.select(abs($"Value"-$"Date"*a + b))
val df_revised=ddf.withColumnRenamed(ddf.columns(0),"Deviation")
//Take the top (k=2) values in Deivation
val max_values=df_revised.sort(desc("Deviation")).take(2)
//Map the df to rdd of DOuble. 
val z=max_values.map(_.getDouble(0)).toList
surprise_values_list+=z(0)
surprise_values_list+=z(1)
}

//Check for Normality 
// \mean+-3*\sigma
val input_values=sc.parallelize(surprise_values_list.toList)
val input_x=surprise_values_list.filter(x=>x< (input_values.mean+3*input_values.stdev)|x>(input_values.mean-3*input_values.stdev))

val n_elements=input_x.length

def find_median(FreqColumn:RDD[Double],low:Int,high:Int,mid:Int):(Double,Double)={
if(high%2==0){
val initial_median=FreqColumn.takeOrdered(n_elements).zipWithIndex.filter(x=>x._2==mid).map{case (a,b)=>a}.head
val revised_median=FreqColumn.takeOrdered(n_elements).map(x=>math.abs(x-initial_median)).sorted.zipWithIndex.filter(x=>x._2==mid).map{case (a,b)=>a}.head
(initial_median,revised_median)
}
else{
//If the n_elements is even Median is average of mid+1 and mid-1	
val initial_median=math.floor(FreqColumn.takeOrdered(n_elements).zipWithIndex.filter(x=>x._2==mid+1 || x._2==mid-1).map{case (a,b)=>a+0}.reduce(_+_)/2).toInt
val revised_median=FreqColumn.takeOrdered(n_elements).map(x=>math.abs(x-initial_median)).sorted.zipWithIndex.filter(x=>x._2==mid+1 || x._2==mid-1).map{case (a,b)=>a+0}.reduce(_+_)/2.toInt
(initial_median,revised_median)
}
}
val low=0
val high=n_elements-1
//The Function takes as an RDD dont pass as list. 
val medians=find_median(sc.parallelize(input_x),low,high,Math.floor((low+high)/2).toInt)

val b=1.4826
val threshold_value=medians._2*b
val upper_limit=(medians._1+2*threshold_value)
val lower_limit=(medians._1-2*threshold_value)
//Filter the DataFrame with the upper and lower limits

val outlier_returned=input_x.filter(x=>x> upper_limit || x< lower_limit)

