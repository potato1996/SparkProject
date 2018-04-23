# SparkProject
RBAD Course Project

Created by Hao chen, Dayou Du

## Prerequisties

Spark-2.2.0

Wget

Gzip

## Usage

The project contains four parts of codes:

### 1. Data collect: ${PROJECTROOT}/dataCollect

This part contains the scripts to collect raw data

**How to run?**

Simply run these scripts in a unix-liked shell


### 2. Analysis StackOverflow: ${PROJECTROOT}/stackoverflow

This part contains analysis code of stackoverflow data

**How to run?**

Run the scala script in spark-shell

**NOTE: After all development finish, 2 and 3 will be built as
standalone applications, and build script will be provided**

### 3. Analysis GitHub: ${PROJECTROOT}/github

This part contains analysis code of github data

**How to run?**

Run the scala scrip in spark-shell in following order:

loadGitHubData.scala

parseRepoLang.scala

parseEvents.scala

scoreGitHub.scala

**NOTE: After all development finish, 2 and 3 will be built as
standalone applications, and build script will be provided**

### 4. Web UI part: ${PROJECTROOT}/Spark-UI

This part is developed as a git-submodule

Please refer to the `README.md` in this folder
