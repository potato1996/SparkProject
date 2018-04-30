# SparkProject
RBAD Course Project

Created by Dayou Du, Hao Chen

## Prerequisties

- Spark-2.2.0

- Wget

- Gzip 

- sbt (on DUMBO, simply `module load sbt`)

**MONGODB IS REQUIRED IF CONNECTING SPARK AND UI MODULE**

The spark program will feed UI module using MongoDB.

Although the transfered data is really small (about several MBs),
it is an elegent and automatic way to pass the data instead of 
passing raw text files.

So, if you are connecting Spark+UI, please set up a MongoDB and set the
corresponding configuration(details are described in the Usage below).

It's OK if you would like to run Spark part ONLY. If the corresponding
configurations are not set, the program will write results in to a TestOut
folder on HDFS.

## Usage

The project contains three parts of codes:

### 1. Data collect: ${PROJECTROOT}/dataCollect

This part contains the scripts to collect raw data.

These scripts will download the raw data and put them onto HDFS.

**Warning**

Extremely LARGE, will need about 48h in total for download and copy to HDFS

**How to run?**

Simply run these scripts in a unix-liked shell

### 2. Analysis StackOverflow and GitHub: ${PROJECTROOT}/src/main/scala

This part contains analysis code of StackOverflow&GitHub data.

#### StackOverflow Part

1. scoreStackOverflow.scala: 

	Read the StackOverflow data, get the tags, filter using language list 
	and technology list, aggregate and convert to score.

#### GitHub Part

1. loadGitHubData.scala

	Read the GitHub Events Timeline data, extract the fields we are interested,
	filter using event-type list.

	Read the GitHub Repo-Language Dataset.

2. parseRepoLang.scala

	Select the major programming language for each repo. Filter out the dummy repos.

3. parseEvents.scala

	For each type of events, tag the event using repo-lang dataset, aggregate and
	convert to score.

4. scoreGitHub.scala 

	Entry point for the GitHub part. Call other functions.

#### Common Codes

1. Common.scala

	Provide the common helper functions to score a language/technology

	Provide Language List, Technology List, and corresponding tag Lists.

2. SparkApp.scala

	Entry point of the whole program. Get the score list from GH/SF, 
	calculate combined score, and then write to Disk/MongoDB

**(For Graders)Where are the inputs?**

1. GitHub Events(Cleaned):

		hdfs:///user/dd2645/SparkProject/CleanedEvents/*

**Note1:** To produce these cleaned events from raw data, please check 
and uncomment the corresponding codes in `scoreGitHub.scala`. Then the 
program will run from raw data, locate at:

		hdfs:///user/dd2645/github_raw/after2015/*

**Note2:(If you choose to run from raw data)** 
It's strongly suggested that you first save the cleaned data,
then run the else parts. The clean procedure will read and parse 3TB
of json files and will take a REALLY LONG TIME - **About 20 hours**.

2. GitHub repo-language dataset:

		hdfs:///user/dd2645/github_repo_language/github.json

3. StackOveflow Posts:

		hdfs:///user/hc2416/FinalProject/Posts.xml

**How to run?**

1. check package versions in ${PROJECTROOT}/build.sbt are correct.
(should be fine on DUMBO)

2. check `sbt.version` in ${PROJECTROOT}/project/build.properties is correct.
(should be fine on DUMBO)

3. goto ${PROJECTROOT}, ensure that you have `sbt` (`module load sbt` on DUMBO)

4. type `sbt assembly`, which will generate a `.jar` package under ${PROJECTROOT}/target/scala-2.11/

5. submit the job: 

If you would like to connect to our UI module, please set `spark.MONGO_URI`, like:

```
spark2-submit --conf "spark.MONGO_URI=mongodb://{username}:{passwd}@{serverIP}:{portNum}/{dbname}" \
              --conf "spark.network.timeout=1200s" \
              --conf "spark.dynamicAllocation.maxExecutors=200" \
              --conf "spark.ui.port=10101" \
              --conf "spark.executor.memory=3g" \
              --conf "spark.driver.memory=6g" \
              ./target/scala-2.11/PotatoFinalProject-assembly-1.0.jar

```

If you would like to run Spark part ONLY, just ignore the `spark.MONGO_URI` configue, like:

```
spark2-submit --conf "spark.network.timeout=1200s" \
              --conf "spark.dynamicAllocation.maxExecutors=200" \
              --conf "spark.ui.port=10101" \
              --conf "spark.executor.memory=3g" \
              --conf "spark.driver.memory=6g" \
              ./target/scala-2.11/PotatoFinalProject-assembly-1.0.jar
```

**IMPORTANT NOTES**

The program is pretty large and would run 30~60 min given commands above. 

`spark.network.timeout` and `spark.executor.memory` is important here.
(should be set at least as much as the example above).

`spark.dynamicAllocation.maxExecutors` is set because by default Spark
would spawn as much executors as possible. But we do NOT want to occupy
all the CPU resources. (which would be killed by Admin :P)


### 3. Web UI part: ${PROJECTROOT}/Spark-UI

This part is developed as a git-submodule

Please refer to the `README.md` in this folder
