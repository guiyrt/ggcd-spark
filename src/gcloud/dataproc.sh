# Google Cloud project and cluster information
PROJECT_ID="ggcd-spark"
CLUSTER_NAME="spark-cluster"
REGION="europe-west2"
ZONE="europe-west2-b"
MASTER_MACHINE_TYPE="e2-standard-4"
WORKER_MACHINE_TYPE="e2-highmem-4"
DISK_SIZE=50
DISK_TYPE="pd-ssd"
WORKERS=2
SECONDARY_WORKERS=2
IMAGE_VERSION="2.0-debian10"
MAX_IDLE_SECONDS="7200s"


E_INVALID_ARGUMENTS=128
function argcEq () {
  if [ "$1" -ne "$2" ]; then
    # shellcheck disable=SC2059
    printf "$3\n"
    exit $E_INVALID_ARGUMENTS
fi
}

function argcGe () {
  if [ "$1" -lt "$2" ]; then
    # shellcheck disable=SC2059
    printf "$3\n"
    exit $E_INVALID_ARGUMENTS
  fi
}

case $1 in

  # Create cluster
  "create")
    argcEq "$#" 1 "Invalid input parameters, create_cluster requires no parameters"
    gcloud beta dataproc clusters create $CLUSTER_NAME --region=$REGION --zone=$ZONE --master-machine-type=$MASTER_MACHINE_TYPE \
           --master-boot-disk-type=$DISK_TYPE --master-boot-disk-size=$DISK_SIZE --num-workers=$WORKERS --worker-machine-type=$WORKER_MACHINE_TYPE \
           --worker-boot-disk-type=$DISK_TYPE --worker-boot-disk-size $DISK_SIZE --num-secondary-workers=$SECONDARY_WORKERS \
           --secondary-worker-boot-disk-type=$DISK_TYPE --secondary-worker-boot-disk-size $DISK_SIZE \
           --image-version=$IMAGE_VERSION --max-idle=$MAX_IDLE_SECONDS --project=$PROJECT_ID \
           --optional-components=HBASE,ZOOKEEPER --enable-component-gateway
    ;;

  # Stop cluster
  "stop")
    argcEq "$#" 1 "Invalid input parameters, stop_cluster requires no parameters"
    gcloud dataproc clusters update $CLUSTER_NAME --region="$REGION" --num-secondary-workers=0
    gcloud dataproc clusters stop $CLUSTER_NAME --region="$REGION"
    ;;

  # Start cluster
  "start")
    argcEq "$#" 1 "Invalid input parameters, start_cluster requires no parameters"
    gcloud dataproc clusters start $CLUSTER_NAME --region="$REGION"
    gcloud dataproc clusters update $CLUSTER_NAME --region="$REGION" --num-secondary-workers=$SECONDARY_WORKERS
    ;;

  # Delete cluster
  "delete")
    argcEq "$#" 1 "Invalid input parameters, delete_cluster requires no parameters"
    gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION
    ;;

  # Submit Spark job
  "spark")
    argcGe "$#" 3 "Invalid input parameters, spark requires at least 2 parameters.\\nUsage: ./dataproc.sh spark <JARS> <MAIN_CLASS> [JOB ARGUMENTS]"
    gcloud dataproc jobs submit spark --region=$REGION --cluster=$CLUSTER_NAME  --jars="$2" --class="$3" -- "${@:4}"
    ;;

  # Submit Hive job
  "hive")
    argcEq "$#" 2 "Invalid input parameters, hive requires 1 parameter.\\nUsage: ./dataproc.sh hive <ACTION_FILE>"
    ACTION=$(cat "$2")
    gcloud dataproc jobs submit hive --region=$REGION --cluster=$CLUSTER_NAME --execute="\"$ACTION\""
    ;;

 # Add file to HDFS
  "hdfs_mkdir")
    argcEq "$#" 2 "Invalid input parameters, hdfs_mkdir requires 1 parameter.\nUsage: ./dataproc.sh hdfs_mkdir <PATH>"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -mkdir $2"
    ;;

  # Add file to HDFS
  "hdfs_upload")
    argcEq "$#" 3 "Invalid input parameters, hdfs_upload requires 2 parameters.\nUsage: ./dataproc.sh hdfs_upload <SRC> <DEST>"
    FILE="$(basename "$2")"
    gcloud compute scp --recurse --zone="$ZONE" "$2" "$CLUSTER_NAME"-m:"$FILE"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -put $FILE $3; rm -r $FILE"
    ;;

  # Download files from HDFS
  "hdfs_download")
    argcEq "$#" 3 "Invalid input parameters, hdfs_download requires 2 parameters.\nUsage: ./dataproc.sh hdfs_download <SRC> <DEST>"
    FILE="$(basename "$2")"
    TIMESTAMP="$(date +%s%N)"
    # shellcheck disable=SC2001
    DEST_PATH="$(echo "$3" | sed 's:/*$::')"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -get $2 $FILE-$TIMESTAMP"
    gcloud compute scp --recurse --zone="$ZONE" "$CLUSTER_NAME"-m:"$FILE"-"$TIMESTAMP" "$DEST_PATH"/"$FILE"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="rm -rf $FILE-$TIMESTAMP"
    ;;

  # Delete file from HDFS
  "hdfs_delete")
    argcEq "$#" 2 "Invalid input parameters, hdfs_delete requires 1 parameters.\nUsage: ./dataproc.sh hdfs_delete <SRC>"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -rm -R $2"
    ;;

  # List directory in HDFS
  "hdfs_ls")
    argcEq "$#" 2 "Invalid input parameters, hdfs_ls requires 1 parameters.\nUsage: ./dataproc.sh hdfs_ls <SRC>"
    gcloud compute ssh --zone="$ZONE" "$CLUSTER_NAME"-m --command="hdfs dfs -ls $2"
    ;;

    *)
      # shellcheck disable=SC2059
      printf "Invalid option!\nUsage: ./dataproc.sh <create|start|stop|delete|spark|hive|hdfs_mkdir|hdfs_upload|hdfs_download|hdfs_delete|hdfs_ls> [ARGS]\n"
      exit $E_INVALID_ARGUMENTS
      ;;
esac