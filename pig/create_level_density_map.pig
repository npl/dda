REGISTER ../bin/dda.jar
register /usr/lib/zookeeper/zookeeper.jar
register /usr/lib/hbase/hbase-0.90.4-cdh3u2.jar
register /usr/lib/hbase/lib/guava-r06.jar


DEFINE TileTableCreateKey dda.pig.udf.TileTableCreateKey();
DEFINE TileTableCreateLevelKey dda.pig.udf.TileTableCreateLevelKey();

set job.name 'aggregate-density-map-level-$zoom_next'

-- load detailed density map
detailed_density_map = LOAD '$output_dir/density-map/$zoom_base/part-*' AS (xId : long, yId : long, density : double);
density_map_tmp1 = FOREACH detailed_density_map GENERATE xId/2 AS xId, yId/2 AS yId, density;
density_map = FOREACH (GROUP density_map_tmp1 BY (xId, yId)) GENERATE FLATTEN(group), SUM(density_map_tmp1.density)/4.0 AS density;

-- find min/max density over all tiles
min_max_density = FOREACH (GROUP density_map ALL) GENERATE MIN(density_map.density) AS min, MAX(density_map.density) AS max;

-- store the density_map in hdfs 
STORE density_map INTO '$output_dir/density-map/$zoom_next';

-- store the density_map in hbase
hbase_density_map = FOREACH density_map GENERATE TileTableCreateKey(xId, yId, (long)$zoom_next), density;
STORE hbase_density_map INTO '$hbase_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('density:_density_');

-- store the min/max values in hbase
hbase_min_max_density = FOREACH min_max_density GENERATE TileTableCreateLevelKey((long)$zoom_next), min, max;
STORE hbase_min_max_density INTO '$hbase_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('density:_min_density_ density:_max_density_');

