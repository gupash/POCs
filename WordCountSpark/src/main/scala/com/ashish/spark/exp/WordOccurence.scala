package com.ashish.spark.exp

import org.apache.spark.{SparkConf,SparkContext}

/**
  * Created by ashish on 17/12/15.
  */
object WordOccurence {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("word-Count").setMaster("local")
    val sc = new SparkContext(conf)

    println("Enter the search Key : ")
    val word = Console.readLine()
    println("You entered : " + word)

    val input = sc.textFile("input/poems_sample.txt")
    val splittedLines = input.flatMap(_.split("\\W+"))
                             .filter(_.equals(word))

    println("$$$$$$$$$$$"+splittedLines.count()+"$$$$$$$$$$$$$")
    splittedLines.saveAsTextFile("output/poems_sample")

  }
}