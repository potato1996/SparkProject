/**
  * Author: Spikerman
  * Created Date: 4/16/18
  */

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

import scala.xml._

object StackOverflow {

  val isLocalMode: Boolean = true

  def hasTag(xmlString: String): Boolean = {
    val tag = XML.loadString(xmlString) \\ "row" \ "@Tags"
    tag.nonEmpty
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val cal = Calendar.getInstance()

  def getPosts(xmlString: String): (String, Set[String]) = {
    val node = XML.loadString(xmlString) \\ "row"

    val tagsString = (node \ "@Tags").text
    val tmpTagSet = tagsString
      .split("(<)|(>)", -1)
      .toSet - "" // remove "<" and ">", remove empty string

    val tags = tmpTagSet.map(tag => tag.toLowerCase) // convert all tags to lowercase

    val dateString = (node \ "@CreationDate").text

    val date = sdf.parse(dateString)

    cal.setTime(date)

    val year = cal.get(Calendar.YEAR)

    val month = cal.get(Calendar.MONTH)

    val season = (month - 1) / 3 + 1

    (year + "" + season, tags)
  }

  //word count function, return the map sorted by value
  def countWords(languages: Iterable[String], date: String) = {

    val map = scala.collection.mutable.Map.empty[String, Double].withDefaultValue(0)

    for (language <- languages) {
      val key = date + ":" + language
      map(key) += 1
    }

    val maxFreq: Double = map.valuesIterator.max
    val factor: Double = 100 / maxFreq

    val rs = map.map(field => (field._1, field._2 * factor))

    //scala.collection.immutable.ListMap(map.toSeq.sortWith(_._2 > _._2): _*)
    rs
  }

  def runStackOverFlow(sourceFile: String, outputFile: String) {
    var sf = sourceFile
    var of = outputFile
    if (isLocalMode) {
      sf = "file:///Users/chenhao/IdeaProjects/Spark/" + sourceFile
      of = "file:///Users/chenhao/IdeaProjects/Spark/" + outputFile
    }
    //configure spark context
    val sparkLocalConf = new SparkConf().setMaster("local[1]").setAppName("StackOverflow")
    //configure mongodb write operation

    val sc = if (isLocalMode) new SparkContext(sparkLocalConf) else new SparkContext()


    // export MONGO_URL= "mongodb://127.0.0.1"
    val writeConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/spark.output"))

    val rowData = sc.textFile(sf).filter(line => line.contains("row"))

    val data = rowData.filter(line => hasTag(line))

    val pairs = data.map(line => getPosts(line))

    val monthlyTags = pairs.map(fields => (fields._1, fields._2))
      .groupByKey()
      .map(fields => (fields._1, fields._2.flatMap(tagSet => tagSet.toList)))

    // tag count map
    val tagsCnt = monthlyTags.map(fields => (fields._1, countWords(fields._2, fields._1)))

    //tagsCnt.flatMap(fields => fields._2).map(fields => "{" + "time: " + fields._1.split(":")(0).toInt + ", " + "language: " + s""""${fields._1.split(":")(1)}"""" + ", " + "score: " + fields._2 + "}").foreach(println)

    val documents = tagsCnt.flatMap(fields => fields._2).map(fields => Document.parse("{" + "time: " + fields._1.split(":")(0).toInt + ", " + "language: " + s""""${fields._1.split(":")(1)}"""" + ", " + "score: " + fields._2 + "}"))

    MongoSpark.save(documents, writeConfig)

    sc.stop()

    println("Success")
  }

  def main(args: Array[String]) {
    val input = "FinalProject/Posts.xml"
    val output = "FinalProject/Result"
    runStackOverFlow(input, output)
  }
}


