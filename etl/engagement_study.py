from pyspark.sql import SparkSession
from utils.helpers import load_main_summary, aggregate_data
import click


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


@click.command()
@click.option('--min-date', required=True)
@click.option('--max-date', required=True)
@click.option('--pcd-min', required=True)
@click.option('--pcd-max', required=True)
@click.option('--channel', default='release')
@click.option('--locale', default='en-US')
@click.option('--country', default='US')
@click.option('--input-bucket', default='telemetry-parquet')
@click.option('--input-predix', default='main_summary')
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

    for sample_id in range(100):

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
