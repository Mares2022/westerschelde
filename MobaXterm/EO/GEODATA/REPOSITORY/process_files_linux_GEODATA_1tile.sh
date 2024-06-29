#!/bin/bash
#cd /home/eodc/EO/GEODATA/
#chmod +x process_files_linux_GEODATA_1tile.sh
#./process_files_linux_GEODATA_1tile.sh
echo "Hello, World!"

# Set the input file containing file paths
inputFile="/home/eodc/EO/GEODATA/INPUT/Area_WF.txt"

# Set the output directory where you want to unzip the files
outputDirectory="/eodc/private/deltares/EO/GEODATA/INPUT/"

acoliteDirectory="/home/eodc/acolite/"

# Create the output directory if it doesn't exist
if [ ! -d "$outputDirectory" ]; then
    mkdir -p "$outputDirectory"
fi

# Loop through each line in the input file
while IFS= read -r sourceFile; do
    
    echo "Source files:"
    echo "$sourceFile"
    
    # Extract the filename from the source path
    filename=$(basename "$sourceFile")

    # Construct the destination path
    destination="$outputDirectory/$filename"
    
    echo "Destination files:"
    echo "$destination"
    
    # Copy the file to the output directory
    cp "$sourceFile" "$destination"
    
    # Change the current directory to the output directory
    cd "$outputDirectory"
    
    # Unzip the file (assuming it's a ZIP archive)
    unzip -q "$filename"
    
    rm "$destination"
    
    fileNameWithoutExt="${filename%.zip}"
    
    s2image="$outputDirectory/$fileNameWithoutExt.SAFE"
    
    echo "$s2image"
    
    echo "Processing Acolite"
    
    cd "$acoliteDirectory"
    python launch_acolite.py --cli --settings=/home/eodc/EO/GEODATA/INPUT/settings_CLI2_GEODATA_WF.txt --inputfile="$s2image"

    rm -r "$s2image"
    
    done < "$inputFile"

echo "Files copied, unzipped, and deleted successfully."


