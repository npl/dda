#!/bin/bash


function die {
	echo $1
	exit 1
}

if [ $# -eq 3 ]; then
	if [ ! -z $JAVA_HOME ]; then
		OSM_FILE="$1"
		BASE_OUTPUT_DIR="$2"
		HBASE_TABLE="$3"
		echo "splitting $OSM_FILE into nodes.txt and wayparts.txt"
		hadoop jar ../bin/dda.jar dda.hadoop.job.Xml2PlainTextJob "$OSM_FILE" "$BASE_OUTPUT_DIR/base" || die "Xml2PlainTextJob failed"
		
		echo "creating density map for layer 18 (base layer)"
		pig -p hbase_table=$HBASE_TABLE -p zoom=18 -p output_dir="$BASE_OUTPUT_DIR" -f create_detailed_density_map.pig || die "detailed density layer creation failed"

		for zoom in $(seq 0 17 | tac); do
			echo "creating density map for layer $zoom"
			pig -p hbase_table=$HBASE_TABLE -p zoom_base=$(($zoom+1)) -p zoom_next=$zoom -p output_dir="$BASE_OUTPUT_DIR" -f create_level_density_map.pig || die "density layer $zoom creation failed"
		done
	else
		die "you need to set JAVA_HOME to your java implementation"
	fi
else 
	die "you need to run this script as ./make_density_map.sh <planet.osm> <base-output-dir> <hbase-table>"
fi


