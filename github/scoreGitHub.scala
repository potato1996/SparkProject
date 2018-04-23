//Ignore chatty messages :P
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import scala.util.parsing.json.JSON
import org.apache.spark.rdd._

//TODO: warp these up

    val combineScore = (rddList:List[RDD[(String,Map[String, Double])]],
               weightList:List[Double]) => {
            val combineTwo = (rdd1: RDD[(String, Map[String, Double])],
                              rdd2: RDD[(String, Map[String, Double])],
                              weight1: Double,
                              weight2: Double) => {
                //semiAdd with weights:
                val semiAdd = (m1:Map[String, Double], 
                               m2:Map[String, Double],
                               w1:Double,
                               w2:Double) => {
                    m1.map{case(k, v) => k -> w1 * v} ++ 
                    m2.map{case(k, v) => k -> (w2 * v + w1 * m1.getOrElse(k, 0.0))}
                }
                rdd1.join(rdd2).mapValues(p => semiAdd(p._1, p._2, weight1, weight2));
            };
            rddList.zip(weightList).
                   reduce((p1:(RDD[(String, Map[String,Double])], Double), 
                           p2:(RDD[(String, Map[String,Double])], Double)) =>
                            (combineTwo(p1._1, p2._1, p1._2, p2._2), p1._2 + p2._2))._1;
            
        }

    val transToPercent = (countData:RDD[(String,Map[String,Long])]) => {
            //first, calculate sum of count
            val sumAll = countData.mapValues(m => (m, m.values.reduce(_+_)));
            
            sumAll.mapValues(p => p._1.mapValues(v => 100.0 * v / p._2));
        }

    def runGitHub():Unit = {
        //load github events
        //for now just test on monthly data - much faster :P
        val EventPath = "hdfs:///user/dd2645/github_raw/after2015/2018-*";
        val allEvents = loadGitHubEvents(EventPath,sc);

        //load github repo language
        //original json data from google big query open data set
        val repoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
        val repoLang = loadRepoLang(repoLangPath,sc);
    
        //Get the top first language from each repo
        val repoMainLang = selMainLang(repoLang);
        repoMainLang.persist();

        //Get the instrested language list
        //val topLangList = getTopLangList(repoMainLang, 100);
        val topLangList = langList;

        val pushReduced = getNumPush(allEvents, 
            repoMainLang, truncToSeason).
            mapValues(m => m.filterKeys(topLangList.contains(_)));

        val pushPercentage = transToPercent(pushReduced);

        //test output
        val outputDir = "hdfs:///user/dd2645/SparkProject/testOut";
        pushPercentage.map(p => "(" + p._1.toString + "," + p._2.toString + ")").coalesce(1).saveAsTextFile(outputDir);
    }


runGitHub()

