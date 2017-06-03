import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import twitter4j.auth.OAuth2Authorization
import twitter4j.conf.ConfigurationBuilder

object TwitterStream {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage TwitterStream <frequency> <window>")
      System.exit(1)
    }

    // Read the first two args to be the consumer secret and the access token secret
    val Array(freqStr, windowStr) = args.take(2)

    // Configure spark
    // Set the application name
    // Master will be set on the command-line when running
    val sparkConf = new SparkConf().setAppName("twitter-stream-toptags-example")

    // Create a spark context next
    val sparkContext = new SparkContext(sparkConf)
    // And then the streaming context. Note here we create
    //   - a window 10 seconds in duration
    //   - a frequency 1 second in duration
    // This means we will examine a window of the last 10 seoncds of the data
    // with a frequency of 1 second (i.e. recompute every 1 second)
    val frequency = Seconds(Integer.parseInt(freqStr))
    val window = Seconds(Integer.parseInt(windowStr))
    val streamingContext = new StreamingContext(sparkContext, frequency)

    // Create a twitter stream from the streaming context
    // oauth configuration is provided via twitter4j.properties at the root level
    val tweets: DStream[Status] = TwitterUtils.createStream(streamingContext, None)
    // Figure out the place and text for the tweet.
    val placesAndText = tweets.map(tweet => (tweet.getPlace, tweet.getText))

    // Figure out the country for the tweet if the place is not null.
    val countries = placesAndText.map(pair => pair._1).filter(place => place != null).map(_.getCountry)
    // Parse the tweets for those tokens which start with "#" and at least 2 chars.
    val tweetText = placesAndText.map(pair => pair._2)
    val hashTags = tweetText.flatMap(_.split(" ")).filter(_.startsWith("#")).filter(_.length() > 1)

    // count the countries seen and sum over the time window
    val countryCountsWindow = countries.map(country => (country, 1)).
      reduceByKeyAndWindow((a: Int, b: Int) => a + b, window, frequency)
    // count the tag occurrences over the window every frequency secs
    val hashTagCounts10s = hashTags.map(tag => (tag, 1)).
      reduceByKeyAndWindow((a: Int, b: Int) => a + b, window, frequency)


    // Sort in reverse order. see note below
    val sortedCountryCountsWindow = countryCountsWindow.transform(rdd =>
      rdd.sortBy(countryCountPair => countryCountPair._2, false))
    // hashTagCounts10s will give a new RDD every second. Execute a transform on each
    // of the RDDs to get hashtags sorted by count for every iteration.
    // passing 'false' to the sortBy call sorts in reverse order
    val sortedHashTagCountsWindow = hashTagCounts10s.transform(rdd =>
      rdd.sortBy(hashTagCountPair => hashTagCountPair._2, false))

    // iterate and print the top 15 countries
    sortedCountryCountsWindow.foreachRDD(rdd => {
      val top15countries = rdd.take(15)
      val timeS = System.currentTimeMillis()
      println(s"\n$timeS: Top countries in the past $window (%s total)".format(rdd.count()))
      top15countries.foreach{case (country, count) => println("%s (%s tweets)".format(country, count))}
    })

    // iterate over each of the rdds and take the top-10 items
    sortedHashTagCountsWindow.foreachRDD(rdd => {
      val top10Tags = rdd.take(10)
      val timeS = System.currentTimeMillis()
      println(s"\n$timeS: Popular hashtags in the past $window (%s total)".format(rdd.count()))
      top10Tags.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
