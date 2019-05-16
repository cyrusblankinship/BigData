from pyspark import SparkContext
from os import sys
from datetime import datetime
import geopandas as gpd

# We construct an R-Tree by going through the geometries of the
# geojsons (i.e. the polygons in the 'geometry' column). We only 
# use the bounds, not the actual geometry, and the key for each 
# bound is the index into the neighborhood name
# We construct an R-Tree by going through the geometries of the
# geojsons (i.e. the polygons in the 'geometry' column). We only
# use the bounds, not the actual geometry, and the key for each
# bound is the index into the neighborhood name
def create_index(zones_):
    import rtree
    import fiona.crs
    index = rtree.Rtree()
    for idx, geometry in enumerate(zones_.geometry):
        index.insert(idx, geometry.bounds)
    return index

# This is used in the process_tweets function (below)
def find_zone(p, index, zones_):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones_.geometry[idx].contains(p):
            return idx
    return None

# We perform the same task using Spark. Here we run the task in
# parallel on each partition (chunk of data). For each task, we
# have to re-create the R-Tree since the index cannot be shared
# across partitions. Note: we have to import the package inside
# process_tweets() to respect the closure property.
def process_tweets(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom

    # These are the zones you'll look at
    zones2 = zones_sc.value
    # print(type(zones2))
    index2 = create_index(zones2)

    # Concat two lists of drug related keywords into one single list
    # need to pad the drugs so that it doesn't catch single letters (e.g. 'c')
    drugs = open('drug_illegal.txt', 'r')
    druglist1 = [' {} '.format(x.strip()) for x in drugs]
    drugs2 = open('drug_sched2.txt', 'r')
    druglist2 = [' {} '.format(x.strip()) for x in drugs2]
    druglist = druglist1 + druglist2

    # loop through records
    reader = csv.reader(records, delimiter='|')
    counts = {}
    for row in reader:
        # This is the extracted words in the tweet
        words = ' {} '.format(row[-1])
        # Check if there are any drug related keywords
        if any(drug in words for drug in druglist):
            try:
                # Create Shapely point for spatial join
                point = geom.Point(float(row[2]), float(row[1]))
            except:
                continue
            # Spatial join to census tract
            tract = find_zone(point, index2, zones2)
            if tract:
                counts[(zones.iloc[tract].plctract10, zones.iloc[tract].plctrpop10)] = \
                    counts.get((zones.iloc[tract].plctract10, zones.iloc[tract].plctrpop10), 0) + 1
        else:
            continue
    return counts.items()

def main(sc):
    num_parameters = sc.defaultParallelism * 3
    print("Number of partitions: {}".format(num_parameters))
    drug_tweets = sys.argv[1]  # type: file
    out_file = sys.argv[2]  # type: path
    twitter = sc.textFile(drug_tweets, use_unicode=True,  minPartitions=num_parameters)
    top_tracts = twitter.mapPartitionsWithIndex(process_tweets)\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x: (x[0][0], (x[1] / x[0][1])))\
            .sortByKey()\
            .coalesce(1)

    print (top_tracts)
    top_tracts.saveAsTextFile(out_file)

if __name__ == '__main__':
    start = datetime.now()
    print("initiating spark context...")
    sc = SparkContext()
    print("loading in tracts...")
    zones = gpd.read_file('500cities_tracts.geojson')
    start_length = len(zones)
    # Don't want to look at tracts with populations less than 1 or with invalid geometries

    zones = zones.loc[(zones.plctrpop10.apply(lambda x: bool(int(x)))) & \
                (zones.geometry.apply(lambda x: x.is_valid)), :].reset_index(drop=True)

    end_length = len(zones)
    print("{} tracts --> {} tracts".format(start_length, end_length))
    zones_sc = sc.broadcast(zones)
    main(sc)
    end = datetime.now()
    print ("Total Run Time: {} minutes".format((end-start).total_seconds()/60))