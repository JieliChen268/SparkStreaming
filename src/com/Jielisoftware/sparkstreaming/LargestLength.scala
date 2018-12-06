package com.Jielisoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._

// uses thread-saft counters to keep track of the average length of 
// tweets in a stream

object LargestLength {
  
  // main function when the the action happens
  def main(args: Array[String]) {
    
    // configure weitter credential using twitter.txt
    setupTwitter()
    
    // Set up a spark streaming context named "AverageTweetLength" that run locally
    // all CPU cores and one-second batches of data
    
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    
    // get rid of log spam(this step should be done righ after the context is et up)
    setupLogging()
    
    // Create a Dstream from Twitter using out streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // extract the test of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // map this to tweet character lengths
    val lengths = statuses.map(status => status.length())
    
    // Since we could have multiple processes adding into these running totals 
    // at the same time, we will use java AtomicLong class to make sure these 
    // counters are thread-safe
    
    var totalTweets = new AtomicLong(0)
    var totalChars = new AtomicLong(0)
    var longest = new AtomicLong(0)
    
    lengths.foreachRDD((rdd, time) => {
      
      var count = rdd.count()
   
      if (count > 0) {
        totalTweets.getAndAdd(count)
        totalChars.getAndAdd(rdd.reduce((x,y) => x + y)) 
        longest.getAndAdd(rdd.max())
        
        println("Total tweets : " + totalTweets.get() + 
            " Total characters : " + totalChars.get() + 
            " largestLengh : " + longest.get())
      }
      
    })
    
    // set a checkpoint directory, and kick it all off
    // I could watch this all the time
    ssc.checkpoint("/Users/jielichen/Spark_Steaming/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}