/* WeblogChallenge.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.github.nscala_time.time.Imports._

object WeblogChallenge {
  def main(args: Array[String]) {

    val projectRoot = "/home/josh/Career/technical_questions/WeblogChallenge"
    val logFile = projectRoot + "/data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
    val conf = new SparkConf().setAppName("Weblog Challenge")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()

    // For each line (i.e. log entry), extract IP and timestamp
    val users = logData.map(line => {
            val logEntry = line.split(" ") // per AWS documentation: space-delimited fields (consistent order)
            val timestamp = new DateTime(logEntry(0))
            // for each line, output (IP, (time, time)) (first is start time, second is end time)
            (logEntry(2).split(":")(0), (timestamp, timestamp)) // drop the port from the IP
        }).groupByKey()

    // thin slice: we should (hopefully) see a collection of our (ts, ts) pairs here
    println(users.take(10)(7))

    // Now, for each IP we should be able to sort and then iterate through their accesses. For each
    // record, check if it is within $SESSION_TIMEOUT of the last one: if so, continue this
    // session; else start a new one.
  }
}
