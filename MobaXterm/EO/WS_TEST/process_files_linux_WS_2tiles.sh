#!/bin/bash
#cd /home/eodc/EO/WS/
#chmod +x process_files_linux_WS_2tiles.sh
#./process_files_linux_WS_2tiles.sh
echo "Hello, World!"

# Set the input file containing file paths
inputFile="/home/eodc/EO/WS/INPUT/path_list_three_images.txt"

# Set the output directory where you want to unzip the files
outputDirectory="/eodc/private/deltares/EO/WS/INPUT/"

acoliteDirectory="/home/eodc/acolite/"

# Create the output directory if it doesn't exist
if [ ! -d "$outputDirectory" ]; then
    mkdir -p "$outputDirectory"
fi

# Loop through each line in the input file
while IFS= read -r sourceFile; do

    sourceFile_tile=$(echo "$sourceFile" | sed 's/T31UET/T31UES/')
    echo "Source files:"
    echo "$sourceFile"
    echo "$sourceFile_tile"
    
    # Extract the filename from the source path
    filename=$(basename "$sourceFile")
    filename_tile=$(basename "$sourceFile_tile")

    # Construct the destination path
    destination="$outputDirectory/$filename"
    destination_tile="$outputDirectory/$filename_tile"
    
    echo "Destination files:"
    echo "$destination"
    echo "$destination_tile"
    
    # Copy the file to the output directory
    cp "$sourceFile" "$destination"
    cp "$sourceFile_tile" "$destination_tile"
    
    
    # Change the current directory to the output directory
    cd "$outputDirectory"
    
    # Unzip the file (assuming it's a ZIP archive)
    unzip -q "$filename"
    unzip -q "$filename_tile"
    
    rm "$destination"
    rm "$destination_tile"
    
    fileNameWithoutExt="${filename%.zip}"
    fileNameWithoutExt_tile="${filename_tile%.zip}"
    
    s2image="$outputDirectory/$fileNameWithoutExt.SAFE"
    s2image_tile="$outputDirectory/$fileNameWithoutExt_tile.SAFE"
    
    echo "$s2image"
    echo "$s2image_tile"
    
    echo "Processing Acolite"
    
    cd "$acoliteDirectory"
    python launch_acolite.py --cli --settings=/home/eodc/EO/WS/INPUT/settings_CLI2_changed_limits_60m.txt --inputfile="$s2image,$s2image_tile"

    rm -r "$s2image"
    rm -r "$s2image_tile"
    
    done < "$inputFile"

echo "Files copied, unzipped, and deleted successfully."


