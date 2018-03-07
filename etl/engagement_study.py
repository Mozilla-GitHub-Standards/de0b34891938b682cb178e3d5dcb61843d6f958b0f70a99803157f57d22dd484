from pyspark.sql import SparkSession
import click
import pyspark.sql.functions as fun
import pyspark.sql.types as st
import datetime as dt
import time
from collections import Counter


fields = [
    "client_id",
    "city",
    "os",
    "os_version",
    "profile_creation_date",
    "subsession_start_date",
    "subsession_length",
    "submission_date_s3",
    "sync_configured",
    "app_version",
    "e10s_enabled",
    "e10s_cohort",
    "active_experiment_id",
    "active_experiment_branch",
    "active_addons_count",
    "is_default_browser",
    "default_search_engine",
    "devtools_toolbox_opened_count",
    "places_bookmarks_count",
    "places_pages_count",
    "memory_mb",
    "active_addons",
    "active_theme",
    "telemetry_enabled",
    "search_counts",
    "active_ticks",
    "quantum_ready",
    "scalar_parent_browser_engagement_tab_open_event_count",
    "scalar_parent_browser_engagement_total_uri_count",
    "scalar_parent_browser_engagement_unfiltered_uri_count",
    "scalar_parent_browser_engagement_unique_domains_count",
    "scalar_parent_browser_engagement_window_open_event_count",
    "sample_id"
]


PRE_WINDOW = 28
POST_WINDOW = 28
INSTALL_WINDOW = 14

numeric_fields = [
    "subsession_length",
    "active_addons_count",
    "devtools_toolbox_opened_count",
    "places_bookmarks_count",
    "places_pages_count",
    "search_sources_search_bar",
    "search_sources_url_bar",
    "search_total",
    "search_engines_yahoo",
    "search_engines_google",
    "memory_mb",
    "active_ticks",
    "scalar_parent_browser_engagement_tab_open_event_count",
    "scalar_parent_browser_engagement_total_uri_count",
    "scalar_parent_browser_engagement_unfiltered_uri_count",
    "scalar_parent_browser_engagement_unique_domains_count",
    "scalar_parent_browser_engagement_window_open_event_count"
]

cat_fields = [
    "telemetry_enabled",
    "quantum_ready",
    "is_default_browser",
    "default_search_engine",
    "app_version",
    "e10s_enabled",
    "e10s_cohort",
    "sync_configured",
    "os_version",
    "os",
    "city",
    "has_custom_theme"
]


def construct_search_fields(data):
    data = (
        data
        .withColumn("searches",
                    sum_search_counts("search_counts")))
    return (
        data
        .withColumn("search_sources_search_bar",
                    data.searches.search_sources_search_bar)
        .withColumn("search_sources_url_bar",
                    data.searches.search_sources_url_bar)
        .withColumn("search_sources_follow_on",
                    data.searches.search_sources_follow_on)
        .withColumn("search_total",
                    data.searches.search_total)
        .withColumn("search_engines_yahoo",
                    data.searches.search_engines_yahoo)
        .withColumn("search_engines_google",
                    data.searches.search_engines_google)
        .drop("searches")
        .drop("search_counts"))


def load_main_summary(spark, bucket, prefix, version, min_date, max_date,
                      channel, locale, country, pcd_min, pcd_max, fields):

    pcd_min, pcd_max = [str_to_unix_days(x) for x in (pcd_min, pcd_max)]

    ms = (
        spark
        .read.option("mergeSchema", True)
        .parquet("s3://{}/{}/{}".format(bucket, prefix, version))
        .filter("submission_date_s3 >= '{}'".format(min_date))
        .filter("submission_date_s3 <= '{}'".format(max_date))
        .filter("normalized_channel = '{}'".format(channel))
        .filter("locale = '{}'".format(locale))
        .filter("country = '{}'".format(country))
        .filter("app_name = 'Firefox'")
        .filter("profile_creation_date >= '{}'".format(pcd_min))
        .filter("profile_creation_date <= '{}'".format(pcd_max))
        .select(fields))

    return construct_search_fields(ms)


def aggregate_data(ms, sample_id):
    mss = ms.filter(ms.sample_id == sample_id)
    # find clients with < 5000 pings to avoid huge joins for
    # pesky active clients
    under_5000 = (mss.groupBy("client_id").count()
                     .filter("count < 5000").select("client_id").distinct())
    # we are caching this
    data = (
        mss
        .withColumn("period", get_period_udf("profile_creation_date"))
        .join(under_5000, on="client_id", how="inner")
        .cache())

    # activate cache
    print "COUNT:", data.count()

    data = (
        data.withColumn('subsession_start_date',
                        fun.udf(lambda x: x[:10])('subsession_start_date'))
            .withColumn('has_custom_theme',
                        data.active_theme.addon_id != '{972ce4c6-7e08-4474-a285-3208198ce6fd}'))

    # split into 3 periods based on subsession_start_date
    pre = data.filter("subsession_start_date <= period.pre_end")
    install = (data.filter("subsession_start_date > period.pre_end")
                   .filter("subsession_start_date <= period.install_end"))
    post = (data.filter("subsession_start_date > period.install_end")
                .filter("subsession_start_date <= period.post_end"))

    # filter to clients who have 0 addons in pre_period
    pre_addons = collapse_addons(pre)
    pre = pre.join(pre_addons, on="client_id", how="left").filter("addons is null").drop("addons")
    install = install.join(pre.select("client_id").distinct(), on="client_id", how="inner")
    post = post.join(pre.select("client_id").distinct(), on="client_id", how="inner")

    # separately aggregate metrics pre and post periods (we ignore metrics for install period)
    pre_agg = agg_numeric_fields(pre)
    post_agg = agg_numeric_fields(post)

    pre_agg_cat = agg_cat_fields(pre)
    post_agg_cat = agg_cat_fields(post)

    # separately collect addons for all 3 periods
    install_addons = collapse_addons(install).withColumnRenamed("addons", "addons_install")
    post_addons = collapse_addons(post).withColumnRenamed("addons", "addons_post")

    # join data for pre and post
    # here we ensure that groups have no addons in the pre period
    pre_final = (pre_agg_cat.join(pre_agg, on='client_id')
                            .join(install_addons, on='client_id', how='left')
                            .join(post_addons, on='client_id', how='left'))
    post_final = (post_agg_cat.join(post_agg, on='client_id')
                              .join(install_addons, on='client_id', how='left')
                              .join(post_addons, on='client_id', how='left'))

    # concat pre and post
    pre_final = pre_final.withColumn("period", fun.lit("pre"))
    post_final = post_final.withColumn("period", fun.lit("post"))
    final = pre_final.unionAll(post_final).repartition(10)

    return final


def date_to_str(x):
    try:
        return dt.datetime.strftime(dt.datetime.utcfromtimestamp(x*24*60*60), format="%Y-%m-%d")
    except ValueError:
        return None


def str_to_unix_days(x):
    try:
        d = dt.datetime.strptime(x, '%Y%m%d')
        return time.mktime(d.timetuple()) / 24 / 60 / 60
    except ValueError:
        return None


def get_period(profile_created):
    """
    Define the period to which a ping belongs
    """
    if profile_created:
        try:
            pre_end = profile_created + PRE_WINDOW
            install_end = pre_end + INSTALL_WINDOW
            post_end = install_end + POST_WINDOW
        except ValueError:
            return (None, None, None)
        return (date_to_str(pre_end), date_to_str(install_end), date_to_str(post_end))

    return (None, None, None)


get_period_udf = fun.udf(get_period, st.StructType([
                st.StructField("pre_end", st.StringType()),
                st.StructField("install_end", st.StringType()),
                st.StructField("post_end", st.StringType())
            ]))


def agg_cat_fields(data):
        def get_mode(x):
            ret = ''
            if len(x) > 0:
                try:
                    ret = str(Counter(x).most_common(1)[0][0])
                except UnicodeEncodeError:
                    try:
                        ret = ret.encode('utf8')
                    except Exception:
                        ret = ''
            return ret

        data = data.select(["client_id"] + cat_fields)\
                   .groupBy("client_id")\
                   .agg(fun.collect_list(cat_fields[0]),
                        fun.collect_list(cat_fields[1]),
                        fun.collect_list(cat_fields[2]),
                        fun.collect_list(cat_fields[3]),
                        fun.collect_list(cat_fields[4]),
                        fun.collect_list(cat_fields[5]),
                        fun.collect_list(cat_fields[6]),
                        fun.collect_list(cat_fields[7]),
                        fun.collect_list(cat_fields[8]),
                        fun.collect_list(cat_fields[9]),
                        fun.collect_list(cat_fields[10]),
                        fun.collect_list(cat_fields[11]))

        for i in cat_fields:
            data = data.withColumnRenamed("collect_list({})".format(i), i)
            if i == 'has_custom_theme':
                data = data.withColumn(i, fun.udf(lambda x: any(x), st.BooleanType())(i))
            else:
                data = data.withColumn(i, fun.udf(get_mode, st.StringType())(i))
        return data


def agg_numeric_fields(data):
        data = data.select(["client_id", fun.lit(1).alias("n")] + numeric_fields)\
                   .groupBy("client_id").sum()
        for i in numeric_fields + ['n']:
            data = data.withColumnRenamed("sum({})".format(i), i)
        return data


def collapse_addons(data):
    def distinct_lst(l):
        return list(set(l))

    dl = fun.udf(distinct_lst, st.ArrayType(st.StringType()))

    return data.select("client_id",
                       fun.explode("active_addons").alias("addons"))\
               .filter("addons.is_system = false and addons.addon_id not like '%hotfix%'")\
               .selectExpr("client_id", "addons.addon_id").distinct()\
               .groupBy("client_id").agg(dl(fun.collect_list("addon_id")).alias("addons"))


def _sum_search_counts(search_counts):
    # define source buckets
    search_bar = 0
    url_bar = 0
    follow_on = 0
    total = 0

    # define engines
    yahoo = 0
    google = 0

    if search_counts is not None:
        for i in search_counts:
            source = i.source
            engine = i.engine
            if source in ('urlbar', 'searchbar', 'contextmenu', 'abouthome', 'newtab', 'system')\
               or "follow-on:" in source:
                total += 1
                if source == 'urlbar':
                    url_bar += 1
                elif source == 'searchbar':
                    search_bar += 1
                elif "follow-on:" in source:
                    follow_on += 1
                if engine[:5] == "yahoo":
                    yahoo += 1
                elif engine in {"google", "google-nocodes"}:
                    google += 1
    return search_bar, url_bar, follow_on, total, yahoo, google


sum_search_counts = fun.udf(_sum_search_counts, st.StructType([
            st.StructField("search_sources_search_bar", st.IntegerType()),
            st.StructField("search_sources_url_bar", st.IntegerType()),
            st.StructField("search_sources_follow_on", st.IntegerType()),
            st.StructField("search_total", st.IntegerType()),
            st.StructField("search_engines_yahoo", st.IntegerType()),
            st.StructField("search_engines_google", st.IntegerType())
        ]))


@click.command()
@click.option('--min-date', required=True)
@click.option('--max-date', required=True)
@click.option('--pcd-min', required=True)
@click.option('--pcd-max', required=True)
@click.option('--channel', default='release')
@click.option('--locale', default='en-US')
@click.option('--country', default='US')
@click.option('--input-bucket', default='telemetry-parquet')
@click.option('--input-prefix', default='main_summary')
@click.option('--input-version', default='v4')
@click.option('--output-bucket', default='telemetry-test-bucket')
@click.option('--output-prefix', default='addons/engagement_study')
def main(min_date, max_date, pcd_min, pcd_max, channel, locale,
         country, input_bucket, input_prefix, input_version,
         output_bucket, output_prefix):

    spark = (SparkSession
             .builder
             .appName("engagement_study")
             .getOrCreate())
    
    print "Distributing helper module to cluster..."
    spark.sparkContext.addPyFile('./etl/utils/helpers.py')
  
    print "Loading Main Summary..."
    ms = load_main_summary(spark=spark,
                           bucket=input_bucket,
                           prefix=input_prefix,
                           version=input_version,
                           min_date=min_date,
                           max_date=max_date,
                           channel=channel,
                           locale=locale,
                           country=country,
                           pcd_min=pcd_min,
                           pcd_max=pcd_max,
                           fields=fields)

    version = '-'.join([min_date, max_date, locale, channel])
    
    print "Aggregating Data by Sample ID..."
    for sample_id in range(100):
        print sample_id, 
        agg = aggregate_data(ms, sample_id)

        # save to s3
        (agg.write.format("parquet")
            .save('s3://{}/{}/v{}/sample_id={}'
                  .format(output_bucket,
                          output_prefix,
                          version,
                          sample_id),
                  mode='overwrite'))

        # clear cache
        spark.catalog.clearCache()


if __name__ == '__main__':
    main()
