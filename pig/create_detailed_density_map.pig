REGISTER ../bin/dda.jar
register /usr/lib/zookeeper/zookeeper.jar
register /usr/lib/hbase/hbase-0.90.4-cdh3u2.jar
register /usr/lib/hbase/lib/guava-r06.jar


DEFINE WayPart2TileGridLengths dda.pig.udf.WayPart2TileGridLengths();
DEFINE WeightTypeDensity dda.pig.udf.WeightTypeDensity();
DEFINE PartOfEarthArea dda.pig.udf.PartOfEarthArea();
DEFINE TileTableCreateKey dda.pig.udf.TileTableCreateKey();
DEFINE TileTableCreateLevelKey dda.pig.udf.TileTableCreateLevelKey();

-- DEFINE ColumnFamily InvokeForString('dda.db.hbase.TileTable.getDensityColumnFamily');

-- DEFINE DensityColumn InvokeForString('dda.db.hbase.TileTable.getDensityColumn');
-- DEFINE MinDensityColumn InvokeForString('dda.db.hbase.TileTable.getMinDensityColumn');
-- DEFINE MaxDensityColumn InvokeForString('dda.db.hbase.TileTable.getMaxDensityColumn');

-- DEFINE GetRowKeyForTile InvokeForString('dda.db.hbase.TileTable.getKeyForTile', 'long long long');
-- DEFINE GetRowKeyForTileLevel InvokeForString('dda.db.hbase.TileTable.getKeyForTileLevel', 'long');

set job.name 'detailed-density-map-level-$zoom'

--  load nodes and wayparts
nodes1 = LOAD '$output_dir/base/nodes-*' USING PigStorage(',') AS (n1_id : long, n1_lat : double, n1_lon : double);
nodes2 = LOAD '$output_dir/base/nodes-*' USING PigStorage(',') AS (n2_id : long, n2_lat : double, n2_lon : double);
wayparts = LOAD '$output_dir/base/wayparts-*' USING PigStorage(',') AS (wayId : double, fromId : double, toId : double, type : chararray);

-- join nodes and wayparts to wayparts_joined
wayparts_joined_tmp1 = JOIN wayparts BY fromId, nodes1 BY n1_id;
wayparts_joined_tmp2 = JOIN wayparts_joined_tmp1 BY toId, nodes2 BY n2_id;
wayparts_joined = FOREACH wayparts_joined_tmp2 GENERATE type AS type, n1_lat AS fromLat, n1_lon AS fromLon, n2_lat AS toLat, n2_lon AS toLon;

-- intersect every waypart with the tile grid and generate {(type, xId, yId, length)}
tile_grid_lengths = FOREACH wayparts_joined GENERATE type, FLATTEN(WayPart2TileGridLengths(fromLat, fromLon, toLat, toLon)) AS (xId : long, yId : long, length : double);

-- weight the generated {(type, xId, yId, length)}-tuples by type and generate {(xId, yId, length)}
weighted_tile_grid_lengths = FOREACH tile_grid_lengths GENERATE xId, yId, WeightTypeDensity(type, length) AS weightedLength;

-- sum the lengths in the {(xId, yId, length)}-tuples for each (xId, yId) and divide by the area
density_map_tmp1 = GROUP weighted_tile_grid_lengths BY (xId, yId);
density_map_tmp2 = FOREACH density_map_tmp1 GENERATE FLATTEN(group) AS (xId, yId), SUM(weighted_tile_grid_lengths.weightedLength) AS weightedSum;
density_map = FOREACH density_map_tmp2 GENERATE xId, yId, weightedSum/PartOfEarthArea(xId, yId, (long)$zoom) AS density;

-- find min/max density over all tiles
min_max_density = FOREACH (GROUP density_map ALL) GENERATE MIN(density_map.density) AS min, MAX(density_map.density) AS max;

-- store the density_map in hdfs 
STORE density_map INTO '$output_dir/density-map/$zoom';

-- store the density_map in hbase
hbase_density_map = FOREACH density_map GENERATE TileTableCreateKey(xId, yId, (long)$zoom), density;
STORE hbase_density_map INTO '$hbase_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('density:_density_');

-- store the min/max values in hbase
hbase_max_density = FOREACH min_max_density GENERATE TileTableCreateLevelKey((long)$zoom), min, max;
STORE hbase_max_density INTO '$hbase_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('density:_min_density_ density:_max_density_');


