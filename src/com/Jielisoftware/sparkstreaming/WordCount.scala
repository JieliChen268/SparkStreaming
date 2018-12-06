package com.Jielisoftware.sparkstreaming

import org.apache.spark._

object WordCount {
  
  def main(args: Array[String]) {
    // set up a SparkContext named WordCount that runs locally using all available cores
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    // Create a RDD of lines of text in our book
    val input = sc.textFile("/Users/jielichen/Spark_Steaming/book.txt")
    
    
    // use flatMap to convert this into an rdd of each word in each line
    val words = input.flatMap(line => line.split(' '))
    // Convert these words to lowercase
    val lowerCaseWords = words.map(word => word.toLowerCase())
    // Count up the occurence of each unique word
    val wordCounts = lowerCaseWords.countByValue()
    
    // print the first 20 results
    val sample = wordCounts.take(20)
    
    for ((word, count) <- sample) {
      println(word + " " + count)
    }
    sc.stop()
    
  }
}