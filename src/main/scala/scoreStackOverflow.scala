//Author: Spikerman
//Create Date: 4/16/18
//----------------------------------------------------------------

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.bson.Document

import scala.xml._

object ScoreStackOverflow{
    val hasTag = (xmlString: String) => {
        val tag = XML.loadString(xmlString) \\ "row" \ "@Tags"
        tag.nonEmpty
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    val cal = Calendar.getInstance();

    val getPosts = (xmlString: String) => {
        val node = XML.loadString(xmlString) \\ "row"

        val tagsString = (node \ "@Tags").text
        val tmpTagSet = tagsString
          .split("(<)|(>)", -1)
          .toSet - "" // remove "<" and ">", remove empty string

        // convert all tags to lowercase
        val tags = tmpTagSet.map(tag => tag.toLowerCase)         
        val dateString = (node \ "@CreationDate").text

        val date = sdf.parse(dateString)

        cal.setTime(date)

        val year = cal.get(Calendar.YEAR)

        val month = cal.get(Calendar.MONTH)

        val season = (month - 1) / 3 + 1

        (year + "" + season, tags)
    }

    //word count function, return the map sorted by value
    val countWords = (languages: Iterable[String], date: String) => {

        val mmap = scala.collection.mutable.Map.empty[String, Long].withDefaultValue(0L)

        for (language <- languages) {
            val key = language
            mmap(key) += 1
        }

        mmap.toMap[String, Long]
    }

    def scoreStackOverFlow(sourceFile: String,
                          sc:SparkContext):RDD[(String,Map[String,Double])] = {

        val rowData = sc.textFile(sourceFile).filter(line => line.contains("row"))

        val data = rowData.filter(line => hasTag(line))

        val pairs = data.map(line => getPosts(line))

        val monthlyTags = pairs.map(fields => (fields._1, fields._2))
            .groupByKey()
            .map(fields => (fields._1, fields._2.flatMap(tagSet => tagSet.toList)));

        // tag count map
        val tagsCnt = monthlyTags.map(fields => (fields._1, countWords(fields._2, fields._1)));

        val filtered = tagsCnt.mapValues(m => m.filterKeys(Common.langList.contains(_)));

        val ScorePercentage = Common.transToPercent(filtered);

        return ScorePercentage;
    }
} 
