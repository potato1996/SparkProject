import org.apache.spark.rdd._

//TODO: wrap this up

   val selMainLang = (repoLang: RDD[(String,List[(String, Long)])]) => {
        //From (repo_name:String -> List(language_name:String, count:Long))
        //To   (repo_name:String -> (major_language_name:String, count:Long))
        //Here is a trick, sort by decending order using "-"
        val repoMainLang = repoLang.mapValues(v => v.sortBy( - _._2) match {
            case x :: _ => x;
            case _      => ("None",0L);
        });

        //filter out dummy repos
        repoMainLang.filter(p => p._2._2 > 100);
    }

    //generate top language list.(contains topCount elements)
    val getTopLangList = (repoMainLang:RDD[(String,(String,Long))],
           topCount:Int) => {

        //To List[String]

        val langAsKey = repoMainLang.map(p => p._2._1 -> 1);

        val countLang = langAsKey.reduceByKey(_+_).collect();

        val sorted = countLang.sortWith((t1:(String, Int), t2:(String, Int)) =>
                                             t1._2 > t2._2);

        sorted.take(topCount).toList.map(p => p._1);
    }


