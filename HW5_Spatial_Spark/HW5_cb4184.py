from pyspark import SparkContext
from os import sys
from datetime import datetime


# We construct an R-Tree by going through the geometries of the
# geojsons (i.e. the polygons in the 'geometry' column). We only 
# use the bounds, not the actual geometry, and the key for each 
# bound is the index into the neighborhood name
def create_index(geojson):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geojson).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return index, zones

# This is used in the processTrips function (below)
def find_zone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

# We perform the same task using Spark. Here we run the task in
# parallel on each partition (chunk of data). For each task, we
# have to re-create the R-Tree since the index cannot be shared
# across partitions. Note: we have to import the package inside
# processTrips() to respect the closure property.
def process_trips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index_nb, zones_nb = create_index('neighborhoods.geojson')
    index_br, zones_br = create_index('boroughs.geojson')
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        try:
            ## get origin and destination points if valid row
            origin = geom.Point(proj(float(row[5]), float(row[6])))
            destination = geom.Point(proj(float(row[9]), float(row[10])))
        except:
            continue
        ## spatial intersection of neighborhoods
        nb = find_zone(origin, index_nb, zones_nb)
        ## spatial intersection of destinations
        br = find_zone(destination, index_br, zones_br)
        if nb and br:
            counts[(zones_nb.neighborhood[nb], zones_br.boroname[br])] = \
            counts.get((zones_nb.neighborhood[nb], zones_br.boroname[br]), 0) + 1
        
    return counts.items()


def main(sc):
    num_parameters = sc.defaultParallelism * 3
    taxis = sys.argv[1]
    taxi = sc.textFile(taxis, use_unicode=True, minPartitions=num_parameters)
    print("Number of partitions: {}".format(taxi.getNumPartitions()))
    top_neighborhoods = taxi.mapPartitionsWithIndex(process_trips) \
            .reduceByKey(lambda x,y: x+y) \
            .map(lambda x: (x[0][1], (x[0][0], x[1]))) \
            .groupByKey() \
            .map(lambda x: (x[0], sorted(x[1], key=lambda neighb: neighb[1], reverse=True)[:3])) \
            .collect()
    print (top_neighborhoods)

if __name__ == '__main__':
    start = datetime.now()
    sc = SparkContext()
    main(sc)
    end = datetime.now()
    print ("Total Run Time: {} minutes".format((end-start).total_seconds()/60))