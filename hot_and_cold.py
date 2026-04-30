from pyspark import SparkConf, SparkContext
import requests
def clean(x):
    try:
        val = float(x)
        return None if val in [-9999.0, -99.0] else val
    except:
        return None

def parse(parts):
    return (
        int(parts[0]),          # WBANNO
        int(parts[1]),          # DATE
        clean(parts[5]),        # T_MAX
        clean(parts[6]),        # T_MIN
    )
def classify(record):
    wban, date, tmax, tmin = record

    if tmax is None and tmin is None:
        return None

    if tmax is not None and tmax > 35:
        return ("Hot Day", date, tmax)

    elif tmin is not None and tmin < 10:
        return ("Cold Day", date, tmin)

    return None
if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("NOAA Read") \
        .setMaster("local[*]") \
        .set("spark.hadoop.fs.defaultFS", "hdfs://127.0.0.1:9000")

    wheather_sc = SparkContext("local", "NOAA")
    url = "https://www.ncei.noaa.gov/pub/data/uscrn/products/daily01/2015/CRND0103-2015-TX_Austin_33_NW.txt"
    data = requests.get(url).text.split("\n")
    rdd = wheather_sc.parallelize(data)
    print("total rows:",rdd.count())
    parsed = (
        rdd.map(lambda line: line.split())
           .filter(lambda x: len(x) >= 9)
           .map(parse)
    )

    classified = parsed.map(classify).filter(lambda x: x is not None)
    # sort by date
    result = classified.sortBy(lambda x: x[1])
    # format output like your screenshot
    print("Total rows:", rdd.count())
    print("Parsed rows:", parsed.count())
    print("Classified rows:", classified.count())
    output = result.zipWithIndex().map(lambda x: f"{x[1]+1} {x[0][0]} {x[0][1]} {x[0][2]:.1f}")
    print("\n".join(output.take(20)))
    # Save full result to HDFS
    output.saveAsTextFile("hdfs://127.0.0.1:9000/weather/classified")
    wheather_sc.stop()