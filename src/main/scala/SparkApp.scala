//Author: Spikerman
//Created Date: 4/16/18
//----------------------------------------------------------------

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

import scala.xml._
/*
object StackOverflow {

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
    val sparkLocalConf = new SparkConf().setAppName("StackOverflow")
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
*/

object PotatoTest{
    def runAll(){
        val sc = new SparkContext();

        val GitHubEventPath = "hdfs:///user/dd2645/github_raw/after2015/2018-03-01*";
        val GitHubRepoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
        val GitHubScoreList = ScoreGitHub.scoreGitHub(GitHubEventPath,
                                  GitHubRepoLangPath,
                                  sc);
        val weightList = List(0.25, 0.25, 0.25, 0.25);

        //test output
        val outputDir = "hdfs:///user/dd2645/SparkProject/testOut3";
        val combinedScore = Common.combineScore(GitHubScoreList, weightList);
        combinedScore.map(p => "(" + p._1.toString + "," + p._2.toString + ")").coalesce(1).saveAsTextFile(outputDir);
        sc.stop()
    }
    def main(args: Array[String]) {
        //val input = "FinalProject/Posts.xml"
        //val output = "FinalProject/Result"
        //runStackOverFlow(input, output)
        runAll();
    }
}



