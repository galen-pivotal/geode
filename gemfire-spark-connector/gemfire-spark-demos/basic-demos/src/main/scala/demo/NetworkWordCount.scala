package demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import io.pivotal.gemfire.spark.connector.GemFireLocatorPropKey
import io.pivotal.gemfire.spark.connector.streaming._

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 * <p><p>
 * In order to run it, you will need to start GemFire cluster, and create the following region
 * with GFSH:
 * <pre>
 * gfsh> create region --name=str_int_region --type=REPLICATE \
 *         --key-constraint=java.lang.String --value-constraint=java.lang.Integer
 * </pre> 
 *
 * <p>To run this on your local machine, you need to first run a net cat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/spark-submit --master=local[2] --class demo.NetworkWordCount <path to>/basic-demos_2.10-0.5.0.jar localhost 9999 locatorHost:port`
 * 
 * <p><p> check result that was saved to GemFire with GFSH:
 * <pre>gfsh> query --query="select * from /str_int_region.entrySet"  </pre>
 */
object NetworkWordCount {
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: NetworkWordCount <hostname> <port> <gemfire locator>")
      System.exit(1)
    }

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").set(GemFireLocatorPropKey, args(2))
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")
    
    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val runningCounts = wordCounts.updateStateByKey[Int](updateFunc)
    // runningCounts.print()
    runningCounts.saveToGemfire("str_int_region")
    ssc.start()
    ssc.awaitTermination()
  }
  
}
