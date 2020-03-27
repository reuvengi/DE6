package com.github.mrpowers.my.cool.project

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read
import org.json4s.jackson.JsonMethods._

//class JsonReader {
//  val conf = new SparkConf().setAppName("spark-test").setMaster("local")
//  new SparkContext(conf)

//}

//{"id":0,"country":"Italy","points":87,"title":"Nicosia 2013 Vulkà Bianco  (Etna)","variety":"White Blend","winery":"Nicosia"}

case class Winemag(id: Option[Int], country: Option[String],points: Option[Int], price : Option[Double], title:Option[String], variety:Option[String], winery: Option[String])


object JsonReader{




  def main(args: Array[String]) {

    implicit val formats = DefaultFormats
    //case class Winemag(id: Int, country: String,points: Int,  title:String, variety:String, winery: String)

    //val json = """{"id":0,"country":"Italy","points":87,"title":"Nicosia 2013 Vulkà Bianco  (Etna)","variety":"White Blend","winery":"Nicosia"}"""
    //val x = parse(json).extract[Winemag]
    //println(x)

    val conf = new SparkConf().setAppName("spark-test").setMaster("local");
    val sc = new SparkContext(conf);
    val path = args(0 )
    //val path = "winemag-data-130k-v2.json";
    val rddFromFile = sc.textFile(path)
    //val decodedUser =
    val y = rddFromFile.map{f => implicit val formats = DefaultFormats; parse(f).extract[Winemag]} //.extract[Winemag])
    val z = y.map(println)
    z.collect


  }

}