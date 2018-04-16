//Ignore chatty messages :P
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

//create SQLContext
//import org.apache.spark.sql._
//val sqlCtx = new SQLContext(sc)
//import sqlCtx._

import scala.util.parsing.json.JSON
import org.apache.spark.rdd._

class myDateTime(_year:Int = 1970, _month:Int = 1, _day:Int = 1, _hour:Int = 0){
    val year = _year;
    val month = _month;
    val day = _day;
    val hour = _hour;
    val season = (month-1) / 3 + 1;

    override def toString(): String = 
             "(" + year + ",Q" + season + "," + month + "," + day + "," + hour + ")";
    
    def toDaily(): myDateTime = new myDateTime(year, month, day, 0);

    def toMonthly(): myDateTime = new myDateTime(year, month, 1, 0);

    def toSeasonly(): myDateTime = new myDateTime(year, (month-1)/3 * 3 + 1, 1, 0);

    def equals(that: myDateTime):Boolean = {
        return this.year == that.year && this.month == that.month && this.day == that.day && this.hour == that.hour;
    }

    def == (that: myDateTime):Boolean = this.equals(that);

    def < (that: myDateTime):Boolean = {
        if(this.year != that.year) return this.year < that.year;
        if(this.month != that.month) return this.month < that.month;
        if(this.day != that.day) return this.day < that.day;
        if(this.hour != that.hour) return this.hour < that.hour;
        return false;
    }

    def != (that: myDateTime):Boolean = !(this == that);

    def <= (that: myDateTime):Boolean = (this < that) || (this == that);

    def > (that: myDateTime):Boolean = !(this <= that);

    def >= (that: myDateTime):Boolean = !(this < that);
}

def loadRepoLang(path: String):RDD[(String, List[(String, Long)])] = {
    //load repo lang json files
    val allRepos = sc.textFile(path, 40);

    //parse to following format:
    //(repo_name:String -> List(language_name:String, count:Long))

    val parseToJson = allRepos.map(line => JSON.parseFull(line));
    
    val extracted = parseToJson.map(x => x.get.asInstanceOf[Map[String,Any]]);
    
    val formatted1 = extracted.map(x => x("repo_name").toString -> x("language"));
    
    val formatted2 = formatted1.mapValues( v => v match {
        case l:List[_] => l
        case m:Map[_,_] => List(m)
    });
    
    val formatted3 = formatted2.mapValues(v => v.asInstanceOf[List[Map[String,String]]].map(m => (m("name"), m("bytes").toLong)));

    return formatted3;
}

def selMainLang(repoLang: RDD[(String,List[(String, Long)])]):RDD[(String,(String,Long))] = {
    //From (repo_name:String -> List(language_name:String, count:Long))
    //To   (repo_name:String -> (major_language_name:String, count:Long))
    //Here is a trick, sort by decending order using "-"
    val repoMainLang = repoLang.mapValues(v => v.sortBy( - _._2) match {
        case x :: _ => x;
        case _      => ("None",0L);
    });

    return repoMainLang;
}

def parseTime(ori: String): myDateTime = {
    val year = ori.substring(0, 4).toInt;
    val month = ori.substring(5, 7).toInt;
    val day = ori.substring(8,10).toInt;
    val hours = ori.substring(11,13).toInt;
    return new myDateTime(year, month, day, hours);
}

def loadGitHubEvents(path: String):RDD[String] = {
    //load raw github events api json files
    //val allEvents = sqlCtx.jsonFile(path);

    val allEvents = sc.textFile(path, 40);

    return allEvents;
}

def reduceByTime(timeToMap: RDD[(myDateTime, Map[String, Long])]): RDD[(myDateTime, Map[String, Long])] = {
    
    //semiAdd: (a -> 1, b -> 2) |+| (a -> 2, c -> 3) = (a -> 3, b -> 2, c -> 3)
    def semiAdd(m1:Map[String, Long], m2:Map[String, Long]) = 
           m1 ++ m2.map{ case (k, v) => k -> (v + m1.getOrElse(k, 0L))};

    return timeToMap.reduceByKey(semiAdd);
}

def numPush(allEvents: RDD[String],
            repoMainLang: RDD[(String, (String, Long))]):RDD[(myDateTime,Map[String, Long])] = {
    
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

    println(joined.count);

    //[(time, Map(lang -> 1))]
    val buildMap = joined.map(p => p._2._1 -> Map(p._2._2._1 -> 1L));

    //reduce by day!
    //we could try others later
    val reduced = reduceByTime(buildMap.map(p => p._1.toDaily() -> p._2));

    println(reduced.count);
 
    return reduced;   
}

def runGitHub():Unit = {
    //load github events
    //for now just test on monthly data - much faster :P
    val EventPath = "hdfs:///user/dd2645/github_raw/after2015/2017-04-01-*";
    val allEvents = loadGitHubEvents(EventPath);

    //load github repo language
    //original json data from google big query open data set
    val repoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
    val repoLang = loadRepoLang(repoLangPath);
    
    //Get the top first language from each repo
    val repoMainLang = selMainLang(repoLang);
    repoMainLang.persist();

    val pushReduced = numPush(allEvents, repoMainLang);

    val outputDir = "hdfs:///user/dd2645/SparkProject/testOut1";
    pushReduced.map(p => "(" + p._1.toString + "," + p._2.toString + ")").coalesce(1).saveAsTextFile(outputDir);
}

runGitHub()
