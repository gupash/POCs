import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


object StreamData {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("Spark-Streaming").set("spark.executor.memory" ,"2G").set("spark.driver.memory", "1G")
    val ssc = new StreamingContext(conf, Seconds(1))
    val cb = new ConfigurationBuilder

    Logger.getRootLogger().setLevel(Level.ERROR)

    cb.setDebugEnabled(true).setOAuthConsumerKey("dbLbzxvM3jS6eT4Wvpb64Bp9H")
      .setOAuthConsumerSecret("lMBBpI06CPxPiYJGRfMNERnYN9raRqYVpImvr4PHoOnSUqyqlu")
      .setOAuthAccessToken("413138489-3wgxDi33LRTzyjv24Xq7RbfW0Gh6XP288060H3Us")
      .setOAuthAccessTokenSecret("TKGREEeGoH69NBEKs1ComWzzJbLkPMbzBI9adMuYjFYIY")

    val auth = new OAuthAuthorization(cb.build)

    val filterFields = Array("iphone", "Iphone", "iPhone", "IPHONE", "IPhone")

    val tweets = TwitterUtils.createStream(ssc,Option(auth), filterFields)

    val englishTweets = tweets.filter(_.getLang == "en")

    englishTweets.map(_.toString) print

    ssc.start()
    ssc.awaitTerminationOrTimeout(5000)
  }
}
