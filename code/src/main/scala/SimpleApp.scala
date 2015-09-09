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
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
