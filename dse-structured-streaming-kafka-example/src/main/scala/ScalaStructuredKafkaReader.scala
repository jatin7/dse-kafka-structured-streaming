import breeze.linalg.split
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

import scala.collection.mutable.ListBuffer
//import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._


// https://stackoverflow.com/questions/46714561/jsontostructs-to-row-in-spark-structured-streaming
// https://databricks.com/blog/2017/02/23/working-complex-data-formats-structured-streaming-apache-spark-2-1.html
// https://spark.apache.org/docs/2.2.2/structured-streaming-programming-guide.html#starting-streaming-queries
// For DSE 6.0.2: use Russell Spitzer's build examples to get the right dependencies
// Columns not found in table tradeskeyspace.trades: key, value, topic, partition, offset, timestamp, timestampType
object ScalaStructuredKafkaReader {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new IllegalArgumentException("kafka bootstrap address is required in addr:9092 format")
    }
    val kafkabootstrap = args(0)

    val spark = SparkSession.builder
      .appName("StructuredStreamingKafkaDSE")
      //      .config("spark.master", sparkmaster)
      .getOrCreate()
    import spark.implicits._

    //  price: Normal(10.0D,2.0D) -> double;
    //  quantity: Normal(10000.0D,100.0D); Add(-10000.0D);
    //  net_amount: Normal(100000.0D,1000.0D);
    //  client: WeightedStrings('ABC_TEST:3;DFG_TEST:3;STG_TEST:14');
    //  client_id: HashRange(0,1000000000) -> long;

    //Struct type X
    val structschema = new StructType()
      .add("trade", new StructType()
        .add("price", DoubleType)
        .add("quantity", DoubleType)
        .add("total", DoubleType)
        .add("client", StringType)
        .add("clientid", StringType))

    val tradesStream = spark.readStream
      .option("checkpointLocation", "/tmp/spark/checkpoints")
      .option("spark.streaming.receiver.maxRate", "10")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkabootstrap)
      .option("subscribe", "trade-events")
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", structschema) as "payload")
      .select("payload.trade.*")


    // .select(".*")

    //    kafkastream.printSchema()

    // Log console
    val jsonconsolestream = tradesStream
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(10000L))
      .format("console")
      .option("truncate", false)
      .start()

    // Create separate outlier stream
    val outlierstream = tradesStream
      .filter("quantity < -100 or quantity > 100")
      .withColumn("flagged_reason", lit("outside 1st stddev"))


    // Create source of record stream
    val rawEventStorage = tradesStream
      .writeStream
      .option("checkpointLocation", "/tmp/spark/checkpoints/raw")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(10000L))
      .format("org.apache.spark.sql.cassandra")
      .option("table", "trades").option("keyspace", "tradeskeyspace")
      .start()


    // Log outliers to a separate location for review
    val toFlaggedTable = outlierstream
      .writeStream
      .option("checkpointLocation", "/tmp/spark/checkpoints/outliers")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("org.apache.spark.sql.cassandra")
      .option("table", "flagged_trades").option("keyspace", "tradeskeyspace")
      .start()


    val jsonNotifications = outlierstream
      //      .toJSON.as("value")
      .select(to_json(struct(outlierstream.columns map col: _*)).alias("value"))

    // write flagged records to the database in a special location for review
    val toFlaggedKafkaTopic = jsonNotifications
      .writeStream
      .option("checkpointLocation", "/tmp/kafka/checkpoints/flagged")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkabootstrap)
      .option("topic", "flagged-trades")
      .start()
    //      .outputMode(OutputMode.Append())
    //      .trigger(Trigger.ProcessingTime(10000L))
    //      .option("checkpointLocation", "/tmp/kafka/checkpoints")

    toFlaggedTable.awaitTermination()
    toFlaggedKafkaTopic.awaitTermination()
    jsonconsolestream.awaitTermination()
    rawEventStorage.awaitTermination()


  }

}
