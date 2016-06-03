# Geojson-to-Socrata

## Takes geojson files and uploads them socrata via standard REST API calls

#### There wasn't a good way to programatically upload geojson files to Socrata. This is a work-around. Uses the standard sodapy replace and upsert methods. This script also contains support to receive email reports to let you know if the upload succeeded or failed.

### Steps to use this work around:

1. Take the first line of your geojson file like this:
`head -n3 geoJsonFile.json > geoJsonFileHeader.json`

2. Manually create a new geo dataset in socrata. When it asks for a file, upload your geoJsonFileHeader.json file.

3. Once the geodata dataset is created, grab the 4x4 for the layer (important to note- the dataset 4x4 and the layer 4x4 are two separate things. You want the layer 4x4 as you will be upserting to this). Stick the layer 4x4 for the dataset into the script's configuration file, fieldconfig.yaml

4. Fill out the rest of config files ( fieldconfig.yaml, socrata_config.yaml, email_config.yaml) with your own params. Ie your socrata username/pass, your email server, directory configs, etc. 

6. Feel free to email questions to: janine.heiser@sfgov.org		