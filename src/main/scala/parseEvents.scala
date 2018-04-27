//Author: Dayou Du(2018)
//Email : dayoudu@nyu.edu
//--------------------------------------------------------------------


import org.apache.spark.rdd._
import scala.util.parsing.json.JSON
object parseEvents{
    val parseTime = (ori:String) => {
        val year = ori.substring(0, 4);
        val month = ori.substring(5, 7);
        val day = ori.substring(8,10);
        val hours = ori.substring(11,13);
        val season = (month.toInt - 1) / 3 + 1;
        year + season.toString + 
              month + 
              day + 
              hours;
    }

    val truncToSeason = (dateTime:String) => dateTime.substring(0, 5);

    val truncToMonth = (dateTime:String) => dateTime.substring(0, 7);

    val reduceByTime = (timeToMap: RDD[(String, Map[String, Long])]) => {

        //semiAdd: (a -> 1, b -> 2) |+| (a -> 2, c -> 3) = (a -> 3, b -> 2, c -> 3)
        val semiAdd = (m1:Map[String, Long], m2:Map[String, Long]) =>
            m1 ++ m2.map{ case (k, v) => k -> (v + m1.getOrElse(k, 0L))};

        timeToMap.reduceByKey(semiAdd);
    }

    val aggSpecEvent = (eventName:String,
            allEvents: RDD[(String, String, String, String)],
            repoMainLang: RDD[(String, (String, Long))],
            truncTimePeriod: String => String) => {

        val onlyPushes = allEvents.filter(line => line._4 == eventName);

        //[(event_repo_name, time)]
        val repoNameAsKey = onlyPushes.map(p => p._3 -> parseTime(p._2));

        //[(event_repo_name, (time, (lang, count)))]
        val joined = repoNameAsKey.join(repoMainLang);

        //[(time, Map(lang -> 1))]
        val buildMap = joined.map(p => p._2._1 -> Map(p._2._2._1 -> 1L));

        //reduce as selected time interval
        val reduced = reduceByTime(buildMap.map(p => truncTimePeriod(p._1) -> p._2));

        reduced.mapValues(m => m.filter(kv => kv._2 >= 100));
    }
}
