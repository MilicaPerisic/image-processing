import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, LongType, StringType, StructField, StructType, TimestampType}
object ProduceImages extends App{

  val spark = SparkSession.builder()
    .master("local[5]")
    .appName("ReadImages")
    .getOrCreate()

  val schema = StructType(Array(
    StructField("path", StringType),
    StructField("modificationTime", TimestampType),
    StructField("length", LongType),
    StructField("content", BinaryType)
  ))


  val df = spark.readStream
    .schema(schema)
    .format("binaryFile")
    .load("src/main/resources/images/*.bmp")
    .withColumn("path", element_at(split(col("path"),"/"),-1))
    .select(to_json(struct(col("path"), col("content"))).as("value"))
    .writeStream
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "mpi")
    .start()
    .awaitTermination()



}
