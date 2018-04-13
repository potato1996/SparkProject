//Ignore chatty messages :P
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.parsing.json.JSON
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

//create SQLContext
import org.apache.spark.sql._
val sqlCtx = new SQLContext(sc)
import sqlCtx._

def loadGitHubEvents(path: String):DataFrame = {
    //load raw github events api json files
    val allEvents = sqlCtx.jsonFile(path);
    
    return allEvents;
}

def loadRepoLang(path: String):Rdd = {
    //load repo lang json files
    val allRepos = sc.textFile(path);

    //parse to following format:
    //(repo_name:String -> List(language_name:String, count:Long))
    val parseToJson = allRepos.map(line => JSON.parseFull(line));
    
    val extracted = parseToJson.map(x => x.get.asInstanceOf[Map[String,Any]]);
    
    val formatted1 = extracted.map(x => x("repo_name").toString -> x("language"));
    
    val formatted2 = formatted1.mapValues( v => v match {
        case l:List[Map[String,String]] => l
        case m:Map[String,String] => List(m)
    });
    
    val formatted3 = formatted2.mapValues(v => v.asInstanceOf[List[Map[String,String]]].map(m => (m("name"), m("bytes").toLong)));

    return formatted3;
}

def selMainLang(repoLang: Rdd): Rdd = {
    //From (repo_name:String -> List(language_name:String, count:Long))
    //To   (repo_name:String -> (major_language_name:String, count:Long))
    //Here is a trick, sort by decending order using "-"
    val repoMainLang = repoLang.mapValues(v => v.sortBy( - _._2).head);

    return repoMainLang;
}

def numPush(allEvents: DataFrame, repoMainLang: Rdd):Unit = {
    
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
