# SparkProject
RBAD Course Project

Created by Dayou Du, Hao Chen

## Prerequisties

Spark-2.2.0

Wget

Gzip 

sbt (on DUMBO, simply `module load sbt`)

**MONGODB IS REQUIRED IF CONNECTING SPARK AND UI MODULE**

The spark program will feed UI module using MongoDB.

Although the transfered data is really small (about several kbs),
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

This part contains the scripts to collect raw data

**How to run?**

Simply run these scripts in a unix-liked shell

### 2. Analysis StackOverflow and GitHub: ${PROJECTROOT}/src/main/scala

This part contains analysis code of StackOverflow&GitHub data

**How to run?**

1. check package versions in ${PROJECTROOT}/build.sbt are correct.
(should be fine on DUMBO)

2. check `sbt.version` in ${PROJECTROOT}/project/build.properties is correct.
(should be fine on DUMBO)

3. goto ${PROJECTROOT}, ensure that you have `sbt` (`module load sbt` on DUMBO)

4. type `sbt assembly`, which will generate a `.jar` package under ${PROJECTROOT}/target/scala-2.11/

5. submit the job: 

If you would like to connect to UI module, please set `spark.MONGO_URI`, like:

`spark2-submit --conf spark.MONGO_URI=mongodb://{username}:{passwd}@{serverIP}:{portNum}/{dbname} \`

`              --conf spark.network.timeout=20s \`

`              --executor-memory 3g \`

`              {path-to-the-jar-package}`

If you would like to run Spark part ONLY, just ignore the `spark.MONGO_URI` configue, like:

`spark2-submit --conf spark.network.timeout=20s \`

`              --executor-memory 3g \`

`              {path-to-the-jar-package}`


### 3. Web UI part: ${PROJECTROOT}/Spark-UI

This part is developed as a git-submodule

Please refer to the `README.md` in this folder
