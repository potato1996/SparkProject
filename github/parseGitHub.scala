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

def loadGitHubEvents(path: String):RDD[((Int,Int,Int,Int),String)] = {
    //load raw github events api json files
    //val allEvents = sqlCtx.jsonFile(path);
    val allEvents = sc.wholeTextFiles(path);

    def parseTime(fileName: String): (Int,Int,Int,Int) = {
        val fields = fileName.split(Array('-','.'));
        val year = fields(0).toInt;
        val month = fields(1).toInt;
        val day = fields(2).toInt;
        val hours = fields(3).toInt;
        return (year, month, day, hours)
    }
    val flattened = allEvents.flatMap(t2 => t2._2.split('\n').map(v => parseTime(t2._1) -> v));
    
    return flattened;
}

def loadRepoLang(path: String):RDD[(String, List[(String, Long)])] = {
    //load repo lang json files
    val allRepos = sc.textFile(path);

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
    val repoMainLang = repoLang.mapValues(v => v.sortBy( - _._2).head);

    return repoMainLang;
}

def numPush(allEvents: RDD[((Int,Int,Int,Int),String)],
            repoMainLang: RDD[(String, (String, Long))]):Unit = {
    
}

def runGitHub():Unit = {
    //load github events
    //for now just test on monthly data - much faster :P
    val EventPath = "hdfs:///user/dd2645/github_raw/after2015/2017-01-*";
    val allEvents = loadGitHubEvents(EventPath);

    //load github repo language
    //original json data from google big query open data set
    val repoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
    val repoLang = loadRepoLang(repoLangPath);
    repoLang.persist()
    
    //Get the top first language from each repo
    val repoMainLang = selMainLang(repoLang);
    repoMainLang.persist()

}

runGitHub()
