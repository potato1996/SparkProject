import org.apache.spark.rdd._
import scala.util.parsing.json.JSON

    val parseTime = (ori:String) => {
        val year = ori.substring(0, 4);
        val month = ori.substring(5, 7);
        val day = ori.substring(8,10);
        val hours = ori.substring(11,13);
        val season = (month.toInt - 1) / 3 + 1;
        year + "-Q" + season.toString + 
               "-" + month + 
               "-" + day + 
               "-" + hours;
    }

    val truncToSeason = (dateTime:String) => dateTime.substring(0, 7);

    val truncToMonth = (dateTime:String) => dateTime.substring(0, 10);

    val reduceByTime = (timeToMap: RDD[(String, Map[String, Long])]) => {

        //semiAdd: (a -> 1, b -> 2) |+| (a -> 2, c -> 3) = (a -> 3, b -> 2, c -> 3)
        val semiAdd = (m1:Map[String, Long], m2:Map[String, Long]) =>
            m1 ++ m2.map{ case (k, v) => k -> (v + m1.getOrElse(k, 0L))};

        timeToMap.reduceByKey(semiAdd);
    }

    val getNumPush = (allEvents: RDD[String],
            repoMainLang: RDD[(String, (String, Long))],
            truncTimePeriod: String => String) => {
            //:RDD[(String,Map[String, Long])] = {

        //parse to json object
        val parseToJson = allEvents.map(line => JSON.parseFull(line));

        //formatting
        val extracted = parseToJson.map(x => x.get.asInstanceOf[Map[String, Any]]);

        //[(event_id, event_create_time, event_repo_name, event_type)]
        val selected = extracted.map(m => (m("id"), m("created_at").toString, m("repo").asInstanceOf[Map[String,String]]("name"), m("type")));

        val onlyPushes = selected.filter(line => line._4 == "PushEvent");

        //[(event_repo_name, time)]
        val repoNameAsKey = onlyPushes.map(p => p._3 -> parseTime(p._2));

        //[(event_repo_name, (time, (lang, count)))]
        val joined = repoNameAsKey.join(repoMainLang);

        //[(time, Map(lang -> 1))]
        val buildMap = joined.map(p => p._2._1 -> Map(p._2._2._1 -> 1L));

        //reduce as selected time interval
        val reduced = reduceByTime(buildMap.map(p => truncToMonth(p._1) -> p._2));

        reduced.mapValues(m => m.filter(kv => kv._2 >= 100));
    }
