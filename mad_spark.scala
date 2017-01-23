import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{unix_timestamp, to_date}
import org.apache.spark.sql.types.{StringType,StructType,DoubleType,StructField}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.{Vectors,Vector}

//Loading the titanic dataset
val df = sqlContext.read.format("com.databricks.spark.csv").
option("header","true"). 
option("inferSchema","true").                         
load("Titanic.csv")

//Drop the first index column
val df_rev=df.drop("_c0")

//Check Normality
val mean_Freq=df_rev.select(mean("Freq")).first()(0).asInstanceOf[Double]
val std_Freq=df_rev.select(stddev("Freq")).first()(0).asInstanceOf[Double]


val lmean=v.mean
val lstd=v.stdev
val upper_range=lmean+3*lstd
val lower_range=lmean-3*lstd


val upper_range=mean_Freq+3*std_Freq
val lower_range=mean_Freq-3*std_Freq
val df_revised=df_rev.select("Freq").filter(df_rev("Freq")< upper_range).filter(df_rev("Freq")>lower_range)


val FreqColumn=df_rev.select("Freq").as[Int].rdd
val n_elements=FreqColumn.count.toInt

//Even Case take the two mid values ie) mid+1 and mid-1

def find_median1(FreqColumn:RDD[Int],low:Int,high:Int,mid:Int):(Int,Int)={
if(high%2==0){
val initial_median=FreqColumn.takeOrdered(n_elements).zipWithIndex.filter(x=>x._2==mid).map{case (a,b)=>a}.head
val revised_median=FreqColumn.takeOrdered(n_elements).map(x=>math.abs(x-medians._1)).sorted.zipWithIndex.filter(x=>x._2==mid).map{case (a,b)=>a}.head
(initial_median,revised_median)
}
else{
println(high)	
val initial_median=math.floor(FreqColumn.takeOrdered(n_elements).zipWithIndex.filter(x=>x._2==mid+1 || x._2==mid-1).map{case (a,b)=>a+0}.reduce(_+_)/2).toInt
val revised_median=FreqColumn.takeOrdered(n_elements).map(x=>math.abs(x-medians._1)).sorted.zipWithIndex.filter(x=>x._2==mid+1 || x._2==mid-1).map{case (a,b)=>a+0}.reduce(_+_)/2.toInt
(initial_median,revised_median)
}
}

val medians=find_median1(FreqColumn,0,n_elements-1,Math.floor((low+high)/2).toInt)
val medians=find_median1(v,0,n_elements-1,Math.floor((low+high)/2).toInt)

//Setup b value
val b=1.4826
val threshold_value=medians._2*b
val upper_limit=(medians._1+2*threshold_value)
val lower_limit=(medians._1-2*threshold_value)


//Filter the DataFrame with the upper and lower limits


val outlier_revised=df_rev.select("Freq").filter(df_rev("Freq")> upper_limit || df_rev("Freq")< lower_limit)





def find_med(low:Int,high:Int):(Int,Int)={
val temp=low+high
(temp,temp+1)
}