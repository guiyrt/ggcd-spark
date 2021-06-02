# Any command failure will stop execution
set -e

# Check if got correct number of inputs
if [ "$#" -ne 4 ]; then
  printf "Wrong number of arguments!\nUsage: ./run.sh <title.basics> <title.ratings> <title.principals> <name.basics> \n"
  exit 128 # 128 is invalid arguments error
fi

# Check if input parameters are valid
if ! [ -f "$1" ] && ! [ -f "$2" ] && ! [ -f "$3" ] && ! [ -f "$4" ]; then
  printf "Invalid files!\nUsage: ./run.sh <title.basics> <title.ratings> <title.principals> <name.basics> \n"
  exit 128 # 128 is invalid arguments error
fi

# Define relevant files and paths
CLUSTER="src/gcloud/cluster.sh"
JAR="target/ggcd-spark-1.0.jar"

# Compile and generate JAR
mvn package

# Create cluster
./"$CLUSTER" create

# Create folders in HDFS
./"$CLUSTER" hdfs_mkdir /titleBasics
./"$CLUSTER" hdfs_mkdir /titleRatings
./"$CLUSTER" hdfs_mkdir /titlePrincipals
./"$CLUSTER" hdfs_mkdir /nameBasics

# Upload files to HDFS
./"$CLUSTER" hdfs_upload "$1" /titleBasics
./"$CLUSTER" hdfs_upload "$2" /titleRatings
./"$CLUSTER" hdfs_upload "$3" /titlePrincipals
./"$CLUSTER" hdfs_upload "$4" /nameBasics

# Run hive jobs (Objective 1)
./"$CLUSTER" hive src/hive/titleBasics.hql
./"$CLUSTER" hive src/hive/titleRatings.hql
./"$CLUSTER" hive src/hive/titlePrincipals.hql
./"$CLUSTER" hive src/hive/nameBasics.hql

# Run spark job (Objective 2)
./"$CLUSTER" spark "$JAR" Jobs.ShowRddOperations

# Run spark jobs (Objective 3)
./"$CLUSTER" spark "$JAR" Jobs.CreateActorPage
./"$CLUSTER" spark "$JAR" ActorPage.Actor