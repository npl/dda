# About 

This project extends the OpenStreetMap/JMapViewer project by adding a street density map to
the viewer.

The work started as an university project to get familiar with the hadoop platform. The 
osm planet file is stored in HDFS and processed by hadoop/mapreduce jobs (using custom
mapreduce-jobs and pig). The final density map is stored in an HBase table. The modified
JMapViewer reads street-density values (these street-density values are calculated for
every tile on every zoom level) from HBase and paints them on a semi-transparent osm-map 
using bilinear interpolation.

# SCREENSHOTS
![Europe StreetDensityMap](https://raw.github.com/npl/dda/master/screenshots/osm_density_europe.jpg "Europe StreetDensityMap")
![USA StreetDensityMap](https://raw.github.com/npl/dda/master/screenshots/osm_density_usa.jpg "USA StreetDensityMap")

# INSTALL / USAGE

#### Building

To build it, just 

* start eclipse
* import the project 
* add the following dependencies (from CDH3 - Cloudera's Distribution): commons-logging-1.0.4.jar, commons-logging-api-1.0.4.jar, hadoop-core-0.20.2-cdh3u2.jar, hbase-0.90.3-cdh3u1.jar, zookeeper-3.3.3-cdh3u1.jar, pig-0.8.1-cdh3u1-core.jar
* create a jar file from the sources and save it under 'bin/dda.jar'

#### Creating a StreetDensityMap

Requirements:

* running hadoop cluster (with full CDH3 installation - Cloudera's Distribution)
* hadoop-jars available on the cluster (including hbase ...)

To create the density map, you need to

* upload planet.osm.bz2 to HDFS
* create the hbase table and a column family named 'density' using 'hbase shell' (create table with "> create 'planet', 'density'")
* run 'cd pig; ./make_density_map.sh <hdfs-planet.osm.bz2> <hdfs-output-dir> <hbase-table>'

#### Viewing the StreetDensityMap

To view the density map, you can start the modified JMapViewer with 'cd viewer; ./viewer.sh <server> <hbase-table>'

** NOTE: This works only if you have your own hadoop cluster running -- there is currently no server publicly available **

# License

This work is licensed under the General Public License (GPL).

# Authors

#### OpenStreetMap/JMapViewer project

* (c) 2008 Jan Peter Stotz and others

#### StreetDensityMap (from the dda project):

* (c) 2011 Martin Kustermann
* (c) 2011 Fabian Ritt
* (c) 2011 Daniel Scheib

