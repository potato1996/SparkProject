#!/bin/sh

module load p7zip

mkdir /scratch/hc2416/stackoverflow_raw/

cd /scratch/hc2416/stackoverflow_raw/

wget https://archive.org/download/stackexchange/stackoverflow.com-PostHistory.7z

7za x stackoverflow.com-PostHistory.7z -oPostHistory

wget https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z

7za x stackoverflow.com-Posts.7z -oPosts

wget https://archive.org/download/stackexchange/stackoverflow.com-Tags.7z

7za x stackoverflow.com-Tags.7z -oTags

hdfs dfs -copyFromLocal Posts FinalProject

hdfs dfs -copyFromLocal PostHistory FinalProject

hdfs dfs -copyFromLocal Tags FinalProject

