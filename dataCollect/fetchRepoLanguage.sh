#!/bin/bash
#Author: DayouDu(2018)
#Email : dayoudu@nyu.edu

#github reponame-language dataset
#from google bigquery
#see: https://cloud.google.com/bigquery/public-data/github

mkdir /scratch/dd2645/github_repo_language

cd /scratch/dd2645/github_repo_language

wget -N https://storage.googleapis.com/spikerman_github_data/github.json

hdfs dfs -mkdir github_repo_language

hdfs dfs -put /scratch/dd2645/github_repo_language/github.json github_repo_language
