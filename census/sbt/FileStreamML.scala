package FileStreamML

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

object FileStreamML {

  def main (args: Array[String] ) {
  
    val spark = SparkSession.builder().config("spark.sql.shuffle.partitions",10).appName("file consumer").getOrCreate()

    val model = PipelineModel.load("hdfs://hdpmst:9000/model/adult")

    val schema = new StructType(Array(StructField("age",IntegerType,true), StructField("workclass",StringType), StructField("fnlwgt",DoubleType), StructField("education",StringType), StructField("education-num",DoubleType), StructField("marital_status",StringType), StructField("occupation",StringType), StructField("relationship",StringType), StructField("race",StringType), StructField("sex",StringType), StructField("capital_gain",DoubleType), StructField("capital_loss",DoubleType), StructField("hours_per_week",DoubleType), StructField("native_country",StringType), StructField("income",StringType)))

    val streamio = spark.readStream.schema(schema).csv("hdfs://hdpmst:9000/data/stream")
    val pred = model.transform(streamio)

    def writePred(df: DataFrame, batchID: Long) {
       df.write.mode("overwrite").format("delta").save("hdfs://hdpmst:9000/data/pred") }  // overwrite using format("delta") 

    val strqry = pred.select("age","label","prediction").writeStream.foreachBatch(writePred _).outputMode("append").option("overwriteSchema", "true").option("checkpointLocation","hdfs://hdpmst:9000/data/checkpoint").start()

    strqry.awaitTermination()
  }
}
