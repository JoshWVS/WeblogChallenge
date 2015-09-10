/* WeblogChallenge.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WeblogChallenge {
  def main(args: Array[String]) {

    val projectRoot = "/home/josh/Career/technical_questions/WeblogChallenge"
    val logFile = projectRoot + "/data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
    val conf = new SparkConf().setAppName("Weblog Challenge")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()

    // For each line (i.e. log entry), extract IP and timestamp
    logData.map(line => {
        val logEntry = line.split(" ")
        (logEntry(0), logEntry(2).split(":")(0)) // drop the port from the IP
    }).take(10).foreach(println)
  }
}
