@echo off
setlocal enabledelayedexpansion

rem Set the input file containing file paths
set "inputFile=/home/eodc/path_list.txt"

rem Set the output directory where you want to unzip the files
set "outputDirectory=/home/eodc/INPUT"

rem Create the output directory if it doesn't exist
if not exist "%outputDirectory%" (
    mkdir "%outputDirectory%"
)

rem Loop through each line in the input file
for /f "delims=" %%a in (%inputFile%) do (

	echo %%a
    set "sourceFile=%%a"
    
    rem Extract the filename from the source path
    for %%b in ("!sourceFile!") do set "filename=%%~nxb"

    rem Construct the destination path
    set "destination="%outputDirectory%\!filename!""

    rem Copy the file to the output directory
    copy "!sourceFile!" !destination!
	
	cd /d "%~dp0INPUT"
	unzip !destination!
	
	echo !destination!
	del !destination!
	echo !filename!
	
	rem Example file name with ".zip" extension
	rem set "fileName=!filename!"

	rem Remove the ".zip" extension
	set "fileNameWithoutExt=!filename:.zip=!"
	
	set "s2image="%outputDirectory%\!fileNameWithoutExt!""
	echo !s2image!
	rem rmdir /s /q !s2image!
		
    rem Unzip the file (assuming it's a ZIP archive)
	rem this can be changed by the zip command
    rem "C:\Program Files\7-Zip\7z.exe" x -o"!outputDirectory!" !destination!
	
    rem Unzip the file (assuming it's a ZIP archive) using the unzip command
    rem unzip -d "!outputDirectory!" "!destination!"

    rem If you want to delete the source file after copying and unzipping, uncomment the next line
    rem del "!sourceFile!"
)

echo Files copied, unzipped, and deleted successfully.
pause
