//Ignore chatty messages :P
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

//create SQLContext
import org.apache.spark.sql._
val sqlCtx = new SQLContext(sc)
import sqlCtx._

def loadGitHubEvents(path: String):DataFrame = {
    //load raw github events api json files
    val allEvents = sqlCtx.jsonFile(path).cache()

    //print schema
    allEvents.printSchema;

    //briefly counting different events
    allEvents.groupBy("type").count().show;

    //Check Range Of EventIds
    val ids = allEvents.select("id").map(row => row(0).toString.toLong).toDF("eventId");
    val idRange = ids.agg(min("eventId"),max("eventId")).head;
    print("Event Id Range: " + idRange.toString + "\n");

    //counting the active repos for current time period
    val activeRepos = allEvents.select("repo.*").distinct;
    print("Num of Active Repos = " + activeRepos.count + "\n");

    return allEvents;
}

def loadRepoLang(path: String):DataFrame = {
    //load repo lang json files
    val allRepos = sqlCtx.jsonFile(path);

    //print schema
    allRepos.printSchema;

    //get number of repos we have
    print("Num of Total Recorded Repo = " + allRepos.count + "\n");
    
    return allRepos;
}

def runGitHub():Unit = {
    //load github events
    //for now just test on monthly data - much faster :P
    val EventPath = "hdfs:///user/dd2645/github_raw/after2015/2017-01-*";
    val allEvents = loadGitHubEvents(EventPath);

    //load github repo language
    //from google
    val repoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
    val repoLang = loadRepoLang(repoLangPath);
}

runGitHub()
