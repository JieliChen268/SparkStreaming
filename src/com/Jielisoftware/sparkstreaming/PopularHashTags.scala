package com.Jielisoftware.sparkstreaming


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

// listens to a stream of tweets and keeps track of the most popular hastags over a 5 minute window

object PopularHashTags {
  
  // mian function is where the action happens
  def main(args:Array[String]) {
    
    // configure twitter credentials using twitters.txt
    setupTwitter()
    
    //setup a Spark streaming context named "PopularHashtags" that runs locally using all
    // local CPU cores and on-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up) 
    setupLogging()
    
    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    // eleminate anything that's a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    // map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag,1))
    
    // count them up over a 5 minute window sliding eery one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow((x, y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    
    //   this could also be written in the following shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    
    // sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // print the top 10
    sortedResults.print
    
    // set a checkpoint directory and kick it all off
    // I can watch this all day
    ssc.checkpoint("/Users/jielichen/Spark_Steaming/checkpoint/")
    ssc.start()   // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    
  }
  
}