import pytest
# from helpers.utils import is_same
from pyspark.sql import SparkSession, Row
from etl.utils.helpers import aggregate_data, construct_search_fields


#  Makes utils available
pytest.register_assert_rewrite('tests.helpers.utils')


@pytest.fixture
def spark():
    return SparkSession \
            .builder \
            .appName("engagement_study_tests") \
            .getOrCreate()


@pytest.fixture
def main_summary_data():
    a1 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=True),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    # a2 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
    #           foreign_install=False, is_system=False),
    #       Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
    #           foreign_install=False, is_system=True)]

    t1 = Row(addon_id='some-non-default-theme')
    s1 = [Row(engine='google', source='google', count=1)]

    return (
      ((1, 'SF', 'Mac', 1, 17533, '2018-01-01T000',
        10, '20170101', True, '57', True, 'a', 2,
        True, 'google', 1, 3, 5, 123, a1, t1,
        True, s1, 2, True, 1, 1, 1, 1, 1, 1),
       (1, 'SF', 'Mac', 1, 17533, '2018-01-31T000',
        10, '20170131', True, '57', True, 'a', 2,
        True, 'google', 1, 3, 5, 123, a1, t1,
        True, s1, 2, True, 1, 1, 1, 1, 1, 1),
       (1, 'SF', 'Mac', 1, 17533, '2018-02-10T000',
        10, '20170210', True, '57', True, 'a', 2,
        True, 'google', 1, 3, 5, 123, a1, t1,
        True, s1, 2, True, 1, 1, 1, 1, 1, 1),
       (1, 'SF', 'Mac', 1, 17533, '2018-02-20T000',
        10, '20170220', True, '57', True, 'a', 2,
        True, 'google', 1, 3, 5, 123, a1, t1,
        True, s1, 2, True, 1, 1, 1, 1, 1, 1)),
      ["client_id",
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
       "sample_id"]
          )


def test_aggregate_date(spark, main_summary_data):
    ms = construct_search_fields(spark.createDataFrame(*main_summary_data))
    agg = aggregate_data(ms, sample_id=1)

    assert agg.count() > 0
    assert agg.filter("has_custom_theme = true").count() > 0

    periods = agg.groupby('period').count().toPandas()
    for p in periods.iterrows():
        assert p[1]['count'] > 0
