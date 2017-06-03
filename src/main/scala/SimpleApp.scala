/**
 * SimpleApp.scala
 * Simple application with filters README.md and counts filtered words.
 */
 import org.apache.spark.SparkContext
 import org.apache.spark.SparkContext._
 import org.apache.spark.SparkConf

 object SimpleApp {
    def main(arg: Array[String]) {
        val logFile = "README.md"
        val conf = new SparkConf()
        val sc = new SparkContext(conf)

        val logData = sc.textFile(logFile, 2).cache()
        val numA = logData.filter(line => line.contains("a")).count()
        val numB = logData.filter(line => line.contains("b")).count()
        val numAandB = logData.filter(line => line.contains("a"))
                              .filter(line => line.contains("b"))
                              .count()

        println(s"Lines with a: $numA, Lines with b: $numB, Lines with both: $numAandB")
        sc.stop()
    }
 }
