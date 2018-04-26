import scala.xml._
import java.text.SimpleDateFormat
import java.util.Calendar

def hasTag(xmlString: String): Boolean = {
    val tag = XML.loadString(xmlString) \\"row" \ "@Tags"
    !tag.isEmpty
}

val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
val cal = Calendar.getInstance() 

def getPosts(xmlString: String): (String, Set[String]) = {
    val node = XML.loadString(xmlString) \\"row"
    
    val tagsString = (node \ "@Tags").text
    val tmpTagSet = tagsString.split("(<)|(>)", -1).toSet - "" // remove "<" and ">", remove empty string
    val tags = tmpTagSet.map(tag => tag.toLowerCase) // convert all tags to lowercase

    val dateString = (node \ "@CreationDate").text
    val date = sdf.parse(dateString)
    cal.setTime(date)
    val year = cal.get(Calendar.YEAR);
    val month = cal.get(Calendar.MONTH);    
    
    (year+"-"+month, tags)
}

//word count function, return the map sorted by value
def countWords(tags: Iterable[String]) = {
    val map = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
    for(tag <- tags){
        map(tag) += 1
    }
    //map
    scala.collection.immutable.ListMap(map.toSeq.sortWith(_._2 > _._2):_*)
}


val rowData = sc.textFile("FinalProject/JPPosts.xml").filter(line => line.contains("row"))

val data = rowData.filter(line => hasTag(line))

val pairs = data.map(line => getPosts(line))

val monthlyTags = pairs.map(fields => (fields._1, fields._2)).groupByKey().map(fields => (fields._1, fields._2.flatMap(tagSet => tagSet.toList)))

// scala non-spark word count
val tagsCnt = monthlyTags.map(fields => (fields._1, countWords(fields._2)))

//TODO
