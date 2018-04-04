#!/bin/sh
#Author: DayouDu (2018)
#Email : dayoudu@nyu.edu

#Fetch Data From GitHub Archive
mkdir /scratch/dd2645/github_raw/

cd /scratch/dd2645/github_raw/

wget -N http://data.githubarchive.org/2011-{01..12}-{01..31}-{0..23}.json.gz

wget -N http://data.githubarchive.org/2012-{01..12}-{01..31}-{0..23}.json.gz

wget -N http://data.githubarchive.org/2013-{01..12}-{01..31}-{0..23}.json.gz

wget -N http://data.githubarchive.org/2014-{01..12}-{01..31}-{0..23}.json.gz

wget -N http://data.githubarchive.org/2015-{01..12}-{01..31}-{0..23}.json.gz

wget -N http://data.githubarchive.org/2016-{01..12}-{01..31}-{0..23}.json.gz

wget -N http://data.githubarchive.org/2017-{01..12}-{01..31}-{0..23}.json.gz

wget -N http://data.githubarchive.org/2018-{01..03}-{01..31}-{0..23}.json.gz

gzip -d ./*.gz

cd /scratch/dd2645/

hdfs dfs -put ./github_raw github_raw
