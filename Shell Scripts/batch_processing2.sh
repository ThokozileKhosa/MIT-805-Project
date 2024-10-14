# Directory containing the local batch text files
BATCH_DIR="/mnt/Summer/combined"
#BATCH_DIR="/mnt/Winter/combined"
#BATCH_DIR="/mnt/Fall/combined"
#BATCH_DIR="/mnt/Spring/combined"
#BATCH_DIR="/mnt/Other/combined"

# Loop through each batch file (not directory)
for batch_file in "$BATCH_DIR"/*.txt; do
    # Check if it's a regular file
    if [ -f "$batch_file" ]; then 
        # Get the file name without extension
        batch_name=$(basename "$batch_file" .txt) 
        hdfs_batch_path="/user/thokozile/images/Summer/$batch_name"
        #hdfs_batch_path="/user/thokozile/images/Winter/$batch_name"
        #hdfs_batch_path="/user/thokozile/images/Fall/$batch_name"
        #hdfs_batch_path="/user/thokozile/images/Spring/$batch_name"
        #hdfs_batch_path="/user/thokozile/images/Other/$batch_name"

        # Create the HDFS batch directory
        #hdfs dfs -mkdir -p "$hdfs_batch_path"

        # Copy batch file to HDFS
        echo "Copying $batch_file to HDFS: $hdfs_batch_path"
        if hdfs dfs -put "$batch_file" "$hdfs_batch_path"; then
            # Run MapReduce job
            echo "Running MapReduce for $batch_name"
            # Change HDFS output directory for each season's run
            if hadoop jar /home/thokozile/eclipse-workspace/ImageProcessing/imageprocessing.jar \
                ImageProcessingJob "$hdfs_batch_path" "/user/thokozile/output/Summer/$batch_name"; then
                echo "Processed $batch_name successfully"
            else
                echo "Failed to run MapReduce for $batch_name"
            fi
        else
            echo "Failed to copy $batch_file to HDFS"
        fi
    fi
done
