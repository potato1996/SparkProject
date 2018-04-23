import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import scala.util.parsing.json.JSON

    def loadRepoLang(path: String, sc:SparkContext)
        :RDD[(String, List[(String, Long)])] = {
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
    def loadGitHubEvents(path: String, sc:SparkContext):RDD[String] = {
        //load raw github events api json files
        //val allEvents = sqlCtx.jsonFile(path);

        val allEvents = sc.textFile(path, 40);

        return allEvents;
    }


