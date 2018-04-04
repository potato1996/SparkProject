//create SQLContext
import org.apache.spark.sql._
val sqlCtx = new SQLContext(sc)
import sqlCtx._

def loadGitHubEvents(path: String):DataFrame = {
    //load raw github events api json files
    val allEvents = sqlCtx.jsonFile(path);

    //print schema
    allEvents.printSchema;

    //get all event types
    val allEventTypes = allEvents.select("type").distinct;
    allEventTypes.show;

    //briefly counting different events
    allEvents.groupBy("type").count().show;

    return allEvents;
}

def loadRepoLang(path: String):DataFrame = {
    //load repo lang json files
    val allRepos = sqlCtx.jsonFile(path);

    //print schema
    allRepos.printSchema;

    //get number of repos we have
    allRepos.count;
    
    return allRepos;
}

def runGitHub():Unit = {
    //load github events
    val EventPath = "hdfs:///user/dd2645/github_raw/after2015/2017*";
    val allEvents = loadGitHubEvents(EventPath);

    //load github repo language
    //from google
    val repoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
    val repoLang = loadRepoLang(repoLangPath);
}

runGitHub()
