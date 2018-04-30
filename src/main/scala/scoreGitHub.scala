//Author: Dayou Du(2018)
//Email : dayoudu@nyu.edu
//--------------------------------------------------------------------

import org.apache.spark.SparkContext
import scala.util.parsing.json.JSON
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

object ScoreGitHub{
    def scoreGitHub(EventPath: String, 
                  RepoLangPath: String,
                  sc: SparkContext):List[RDD[(String,Map[String,Double])]] = {
        //load github events
        val EventList = List("PushEvent","PullRequestEvent","IssuesEvent","WatchEvent" );

//----------------------------clean data check point------------------------------------
        //val allEvents = loadGitHub.loadGitHubEvents(EventPath, EventList, sc);

        //allEvents.map(line => line._1 + "," + line._2 + "," + line._3 + "," + line._4)
        //         .saveAsTextFile("hdfs:///user/dd2645/SparkProject/CleanedEvents");
//-------------------------------------------------------------------------------------

        val allEvents = sc.textFile("hdfs:///user/dd2645/SparkProject/CleanedEvents", 400)
                          .map(line => line.split(","))
                          .map(line => (line(0),line(1),line(2),line(3)));

        allEvents.persist(StorageLevel.MEMORY_AND_DISK);

        //load github repo language
        //original json data from google big query open data set
        val repoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
        val repoLang = loadGitHub.loadRepoLang(RepoLangPath, sc);
    
        //Get the top first language from each repo
        val repoMainLang = parseRepoLang.selMainLang(repoLang);
        repoMainLang.persist(StorageLevel.MEMORY_AND_DISK);

        //Get the instrested language list
        val topLangList = Common.langList;

        val aggregated = EventList.
            map(eventName => {
                parseEvents.aggSpecEvent(eventName, allEvents, 
                                       repoMainLang, 
                                       parseEvents.truncToSeason).
                mapValues(m => m.filterKeys(topLangList.contains(_)))});

        val ScorePercentage = aggregated.map(rdd => Common.transToPercent(rdd));

        return ScorePercentage;
    }
}

