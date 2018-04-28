//Author: Spikerman
//Created Date: 4/16/18
//----------------------------------------------------------------

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.xml._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object PotatoTest{
    var writeMongoDB: Boolean = false;
    var MongoDB_URI: String = "";
    val OutDir = "hdfs:///user/dd2645/SparkProject/TestOut";

    def convertAndWrite(res: RDD[(String, Map[String, Double])], 
                       tableName: String){
        val writeConfig = WriteConfig(Map("uri" -> (MongoDB_URI + "." + tableName)));

        val flattened = res.flatMap(m => m._2.toSeq.map(p => {
            val timeStr = m._1;
            val name = p._1;
            val score = p._2;
            val _id = timeStr + name;
            val jsonstr = "{"  + 
                  "_id: "      + s""""${_id}""""     + ", " +
                  "time: "     + s""""${timeStr}"""" + ", " + 
                  "language: " + s""""${name}""""    + ", " + 
                  "score: "    + score + "}";
            jsonstr;
        }));
         
        val documents = flattened.map(line => Document.parse(line));

        MongoSpark.save(documents, writeConfig);
    }
    val convertAndSave = (res: RDD[(String, Map[String, Double])], 
                       tableName: String) => {

        val flattened = res.flatMap(m => m._2.toSeq.map(p => {
            val timeStr = m._1;
            val name = p._1;
            val score = p._2;
            val _id = timeStr + name;
            val jsonstr = "{"  + 
                  "_id: "      + s""""${_id}""""     + ", " +
                  "time: "     + s""""${timeStr}"""" + ", " + 
                  "language: " + s""""${name}""""    + ", " + 
                  "score: "    + score + "}";
            jsonstr;
        }));
         
        flattened.coalesce(5).saveAsTextFile(OutDir + "/" + tableName);
    }

    def runAll(){
        val sc = new SparkContext();
        val conf = new SparkConf();
        
        //ignore chatty messages
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
        
        //set mongodb address
        val dbconf = conf.getOption("spark.MONGO_URI");
        dbconf match{
            case Some(s) => {
                 MongoDB_URI = s;
                 writeMongoDB = true;
                 println("MONGO_URI set. Will try connect to MongoDB:" + MongoDB_URI);
            }
            case _ => {
                 writeMongoDB = false;
                 println("MONGO_URI NOT set. Will try save to dir:" + OutDir);
            }
        }

        //input paths
        val GitHubEventPath = "hdfs:///user/dd2645/github_raw/after2015/2018-03-01*";
        val GitHubRepoLangPath = "hdfs:///user/dd2645/github_repo_language/github.json";
        val SFPostPath = "hdfs:///user/hc2416/FinalProject/Posts.xml";
        
        //Score StackOverflow
        val SFScore = ScoreStackOverflow.scoreStackOverflow(SFPostPath, sc);
        
        //Score GitHub
        val GitHubScoreList = ScoreGitHub.scoreGitHub(GitHubEventPath,
                                  GitHubRepoLangPath,
                                  sc);

        //weights to combine overall score
        val weightList = List(0.4, 0.15, 0.15, 0.15, 0.15);
        //val weightList = List(0.4, 0.2, 0.2, 0.2);

        val combinedScore = Common.combineScore(SFScore._1::GitHubScoreList, weightList);
        //val combinedScore = Common.combineScore(GitHubScoreList, weightList);
        
        val tableNames = List("Tech",
                              "LangCombined",
                              "NumPost",
                              "NumPush",
                              "NumPR",
                              "NumIssue",
                              "NumStar");
        //val tableNames = List("LangCombined", "NumPush", "NumPR", "NumIssue", "NumStar");

        val allTables = SFScore._2 :: (combinedScore :: (SFScore._1 :: GitHubScoreList));
        //val allTables = combinedScore :: GitHubScoreList;
        
        allTables.foreach(_.persist(StorageLevel.MEMORY_AND_DISK));
       
        if(writeMongoDB){
              //allTables.zip(tableNames).foreach(p => convertAndWrite(p._1, p._2));
        }else{
              //convertAndSave(SFScore._2, "Tech");
              allTables.zip(tableNames).foreach(p => convertAndSave(p._1, p._2));
        }

        sc.stop();
    }
    def main(args: Array[String]) {
        runAll();   
    }
}



