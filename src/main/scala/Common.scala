//Author: Dayou Du(2018)
//Email : dayoudu@nyu.edu
//-----------------------------------------------------------------

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

object Common{
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
                           p2:(RDD[(String, Map[String,Double])], Double)) => {
                            (combineTwo(p1._1, p2._1, p1._2, p2._2), p1._2 + p2._2)})._1;

    }

    val transToPercent = (countData:RDD[(String,Map[String,Long])]) => {
            //first, calculate sum of count
            val sumAll = countData.mapValues(m => (m, m.values.reduce(_+_)));

            sumAll.mapValues(p => p._1.mapValues(v => 100.0 * v / p._2));
    }

    val langList = List(
        "JavaScript",
        "Python",
        "Java",
	"Go",
	"Ruby",
	"C++",
	"PHP",
	"TypeScript",
	"C#",
	"C",
	"Shell",
	"Scala",
	"Swift",
	"Rust",
	"D",
	"Objective-C",
	"Objective-C++",
	"Kotlin",
	"Groovy",
	"Lua",
	"Clojure",
	"CoffeeScript",
	"Elixir",
	"Perl",
	"Haskell",
	"Dart",
	"R",
	"Erlang",
	"Matlab",
	"Assembly",
	"Fortran",
	"Lisp",
	"Visual Basic",
	"F#",
	"Scheme",
	"Ada",
	"HTML",
	"Verilog",
	"Emacs Lisp");

    val tagLangList = List(
        "javascript",
        "python",
        "java",
	"go",
	"ruby",
	"c++",
	"php",
	"typescript",
	"c#",
	"c",
	"shell",
	"scala",
	"swift",
	"rust",
	"d",
	"objective-c",
	"objective-c++",
	"kotlin",
	"groovy",
	"lua",
	"clojure",
	"coffeeccript",
	"elixir",
	"perl",
	"haskell",
	"dart",
	"r",
	"erlang",
	"matlab",
	"assembly",
	"fortran",
	"lisp",
	"vb.net",
	"f#",
	"scheme",
	"ada",
	"html",
	"verilog",
	"elisp");


    val techList = List(
        "React.js",
        "Angular.js",
        "Vue.js",
        "Backbone.js",
        "Ember.js",
        "jQuery");

    val tagTechList = List(
        "reactjs",
        "angularjs",
        "vue.js",
        "backbone.js",
        "ember.js",
        "jquery");
}
