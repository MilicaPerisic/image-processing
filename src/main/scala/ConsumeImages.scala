import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{base64, col, from_json}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}

import java.awt.image.BufferedImage
import java.io.{ ByteArrayInputStream, File}
import javax.imageio.ImageIO
import java.util.Base64

object ConsumeImages extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("WriteImages")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")
  val schema = StructType(Array(
    StructField("path", StringType),
    StructField("content", BinaryType)
  ))

  val streamedBMSDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "mpi")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), schema).as("value"))
    .select("value.*")
    .withColumn("content", base64(col("content")))


  val ss = streamedBMSDF
    .writeStream
    .option("checkpointLocation", "src/main/resources/checkpointWrite")
    .foreachBatch {
      (batchDF: Dataset[Row], batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.foreachPartition { (t: Iterator[Row]) => {
            while (t.hasNext) {
              val row: Row = t.next()
              try {
                val mpi = s"${row.getAs[String]("content")}"
                val decoded = Base64.getDecoder().decode(mpi)
                val name = s"${row.getAs[String]("path")}"
                val bufferedImage: BufferedImage = ImageIO.read(new ByteArrayInputStream(decoded))
                ImageIO.write(bufferedImage, "bmp", new File("C:\\Users\\mnp16\\IdeaProjects\\test-spark\\src\\main\\resources\\mpImages\\"+ name))
              } catch {
                case e: Exception =>
                  e.printStackTrace()
              }
            }
          }
          }
        }
    }
    .start()

  ss.awaitTermination()

}
