# Spark: Visualizing the distribution of drug-abuse-related tweets

This Spark script filters 100 million tweets to keep only those containing drug-related terms and spatially joins them with census tracts of the top 500 largest cities in the US. For each census tract, the normalized number of tweets is computed using population data.

The script was run in a cluster, using 10 executors with 5 cores each, and took 17 minutes.

Below is a map of the result for the New York City area, plotted using kepler-gl.

![image](result.PNG)