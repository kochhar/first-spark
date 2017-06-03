# First Spark

Simple project for coding up some spark examples.
1. SimpleApp which reads the README.md file and counts the number of lines containing 'a' and 'b'
2. TwitterStream which reads from a Twitter stream and counts the top countries and the top hashtags in a window of time.

This project is built using sbt. Please ensure you have sbt installed. To install on OSX try

    brew install sbt


## SimpleApp
Code is located at

    src/main/scala/SimpleApp

To build

    sbt package

To run

    spark-submit --class "SimpleApp" --master local[4] target/scala-2.11/first-spark-project_2.11-0.1.jar

In case the spark master is located remotely replace `local[4]` with the correct master address.


## Twitter Stream
This is an example of using spark streaming with the Twitter streaming API. To connect to the twitter API a
`twitter4j.properties` file is required in the project root. The format is

    oauth.consumerKey=****************
    oauth.consumerSecret=*********************************
    oauth.accessToken=**********************
    oauth.accessTokenSecret=********************************************


Code is located at

    src/main/scala/TwitterStream

This app uses several dependencies not available in the spark distro. These need to be package up into one fat jar.
For this the assembly plugin is provided. Build via

    sbt assembly

To run (window of 300 seconds, recomputed every 30 seconds)

    spark-submit --class "TwitterStream" --master local[4] target/scala-2.11/first-spark-project-assembly-0.1.jar 30 300

Again replace `--master <spark_master>` if the spark master is not running locally.
