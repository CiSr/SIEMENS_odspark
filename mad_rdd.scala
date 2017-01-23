import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{unix_timestamp, to_date}
import org.apache.spark.sql.types.{StringType,StructType,DoubleType,StructField}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.rdd.RDD


val input_x=v.filter(x=>x< (v.mean+3*v.stdev)|x>(v.mean-3*v.stdev))

val n_elements=v.count.toInt
def find_median1(FreqColumn:RDD[Double],low:Int,high:Int,mid:Int):(Double,Double)={
if(high%2==0){
val initial_median=FreqColumn.takeOrdered(n_elements).zipWithIndex.filter(x=>x._2==mid).map{case (a,b)=>a}.head
val revised_median=FreqColumn.takeOrdered(n_elements).map(x=>math.abs(x-initial_median)).sorted.zipWithIndex.filter(x=>x._2==mid).map{case (a,b)=>a}.head
(initial_median,revised_median)
}
else{
println(high)	
val initial_median=math.floor(FreqColumn.takeOrdered(n_elements).zipWithIndex.filter(x=>x._2==mid+1 || x._2==mid-1).map{case (a,b)=>a+0}.reduce(_+_)/2).toInt
val revised_median=FreqColumn.takeOrdered(n_elements).map(x=>math.abs(x-initial_median)).sorted.zipWithIndex.filter(x=>x._2==mid+1 || x._2==mid-1).map{case (a,b)=>a+0}.reduce(_+_)/2.toInt
(initial_median,revised_median)
}
}

val b=1.4826
val threshold_value=medians._2*b
val upper_limit=(medians._1+2*threshold_value)
val lower_limit=(medians._1-2*threshold_value)


//Filter the DataFrame with the upper and lower limits


val outlier_revised=v.filter(x=>x> upper_limit || x< lower_limit)
