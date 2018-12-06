# SparkStreaming

The is a SparkStreaming project including some APIs:
1. get the streaming tweets from tweet app
2. use thread-saft counters to keep track of the average length of tweets
3. eleminate anything that's a hashtag, count them up over a 5 minute window sliding every one second,sort the results by the count values and get the top 10 popular tweets with hashtag
4. listens to a stream of tweets and saves them to disk
