from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.functions import explode, to_date, udf
from pyspark.sql.types import IntegerType
from collections import Counter
import pandas as pd
from operator import add

'''
Collection of functions useful for anaylsis pertaining to addons on the
Longitudinal dataset. Adding to this module periodically as I find myself 
repeating blocks of code for simple analysis. All functions have been
tested on a full version of the dataset, however are not robust to many edge 
cases that will surely come up as the Longitudinal dataset changes
over time.

To use in Jupyter / any script
import and assign the following:

import addons
addons.sqlContext = sqlContext
addons.sc = sc
sc.addPyFile('addons.py')

(have yet to find a better way to do this)

TODO:
-Create a top-level function for identifying system add-ons that
 incorporates all the edge cases.

-Change RDD operations to DF operations where possible
'''




# Since the is_system field was not adopted until recently, I
# check against this set of add-on GUIDs for identifying/filtering 
# system add-ons
SYSTEM_ADDONS = \
{
    # system add-ons
    'brotli@mozilla.org',
    'e10srollout@mozilla.org',
    'loop@mozilla.org',
    'malware-remediation@mozilla.org',
    'outofdate-notifications-1.2@mozilla.org',
    'outofdate-notifications-1.3@mozilla.org',
    'outofdate-notifications@mozilla.org',
    'firefox@getpocket.com',
    
    # hotfix
    'firefox-hotfix@mozilla.org',
    
    # test pilot
    '@testpilot-addon',
    'wayback_machine@mozilla.org',
    '@activity-streams',
    'tabcentertest1@mozilla.com',
    'universal-search@mozilla.com'
}
    
    


def get_longitudinal_data(columns, sample=True, with_replacement=False,
                          frac=.1, seed=23):
    '''
    param: columns -- list of strings indicating column name 

    Generates a query and fetches data from longitudinal dataset
    The input parameters should be straightforward. Prints the
    schema after execution and the sample parameters as a reminder
    to the user

    return: Spark DataFrame
    '''
    global sqlContext
    query = 'select\n\t' + ',\n\t'.join(columns) + '\nfrom longitudinal\n'
    df = sqlContext.sql(query)
    if sample:
        df = df.sample(with_replacement, frac, seed=seed)
        
    # print schema and sample parameters for user
    print 'Loaded Data:\n\n'
    df.printSchema()
    frac = '({}%)'.format(frac*100) if sample else ''
    print 'Sample = {} {}'.format(sample, frac)
    
    return df


def num_addons_in_top_n(df, top_addons, not_in_top=False):
    """Count how many profiles have 0, 1, ..., addons
    in top_addons. Or alternatively, how many profiles
    have 0, 1, ..., addons NOT in top_addons (when not_in_top=True)

    param: df -- a version on the longitudinal dataset with the 
                 active_addons field
    param: top_addons -- a list or set of addons (computed prior)
    param: not_in_top -- boolean indicating the count type
                         (count in top (False) vs. count not in top (True))

    return: pandas DataFrame with two columns:
    num_in_top/num_not_in_top, count
    """

    def num_top_by_row(row, top_addons=top_addons, not_in_top=not_in_top):
        """Counts the number of addon ids in (not_in_top==False)
        or not in (not_in_top==True) top_addons for a given row
        """

        if row is not None:
            addon_lst = row.keys()
            result = sum([i in top_addons for i in addon_lst])
            if not_in_top:
                result = len(addon_lst) - result
            return result

    # register UDF
    num_top_by_row = udf(num_top_by_row, IntegerType())
    new_column_name = 'num_not_in_top' if not_in_top else 'num_in_top'

    result = df.filter('size(active_addons) > 0')\
                   .selectExpr('active_addons[1] as addons')\
			   .withColumn(new_column_name, num_top_by_row('addons'))\
			       .select(new_column_name)\
               .groupBy(new_column_name).count()\
               .filter('%s is not null' % new_column_name).collect()

    return pd.DataFrame(map(lambda row: row.asDict(), result))


def get_recent_flattened_addons(df):
    '''Generates a flattened Spark DataFrame of addons data

    param df: version of the longitudinal dataset with active_addons field

    Takes the most recent ping per profile and explodes the list
    of addons, making them independent of client_id. 
    
    return: spark Dataframe with two columns: addon, meta
    meta contains all add-on attributes and can be accessed
    via meta.is_system, meta.install_day, etc. 
    '''

    addon_flat = df.filter("size(active_addons) > 0")\
            .selectExpr("active_addons[1] as addons")\
            .filter("size(addons) > 0") \
            .select(explode('addons').alias("addon", "meta"))
    return addon_flat
            
def addon_freqency_DF(df, include_system=False, group_by='addon'):
    recent_flat_addons = get_recent_flattened_addons(df)
    if not include_system:
        recent_flat_addons = recent_flat_addons.filter(~recent_flat_addons['meta.is_system'])
    recent_addons_count = recent_flat_addons.groupBy(group_by).count().collect()
    
    freq_table  = pd.DataFrame(
        map(lambda row: row.asDict(), recent_addons_count))
    
    # one last check for system addons in pings before
    # integration of is_system variable
    if not include_system:
        not_system = [i not in SYSTEM_ADDONS for i in freq_table.addon]
        freq_table = freq_table[not_system]                                                    
    return freq_table.sort_values(by='count', ascending=False)



def get_active_addons(row, include_system=True):
    '''
    Returns a list of the names of active addons for given row
    in the longitudinal dataset, optionally allowing for a specific index
    defaulting at 0 (most recent). Allows for filtering of system
    addons

    (returns 2D list (every entry for a given profile)
    '''
    #### This doesn't currently allow for a specific index...
    history = []
    if row is not None and row.active_addons is not None:
        n = len(row.submission_date)
        history = [None]*n
        for i in range(n):
            ping = row.active_addons[i]
            #### Don't repeat code if possible.
            history[i] = [ping[addon].name for addon in ping \
                            if include_system or not ping[addon].is_system]
    return history

def format_time(x, clean=False):
    fmt = '%Y-%m-%d'
    return pd.to_datetime(x.split('T')[0], format=fmt) if not clean \
      else pd.to_datetime(x, format=fmt)

def addons_per_day(ping, period=None, facet='normalized_channel'):
    """
    Counts the number of addons per unique submission_date in
    the longitudinal dataset. Intended as a helper function
    to mean_addons_per_day()

    ping:            (Row) One profile (and all pings)
    period:   (tuple|list) start and end date for filtering
                           i.e. ('2016-01-01', '2016-06-01')
 
    return: (2D list, [(TimeStamp, int), (Timestamp, int), ... ,]

    """
    if period:
        assert type(period) in (tuple, list) and len(period) == 2, \
        'Invalid input for <period>, must be a tuple or list of length 2'
        start_date, end_date = [pd.to_datetime(i, format='%Y-%m-%d') for i in period]

    submissions = ping.submission_date
    n = len(submissions)
    date_counts = [None] * n
    
    if ping.submission_date is not None and ping.active_addons is not None:
        for i in xrange(n):
            # extract date and convert to TimeStamp
            date = ping.submission_date[i].split('T')[0]
            date = pd.to_datetime(date, format='%Y-%m-%d')
            if not period or (period and start_date <= date and end_date >= date):
                num_total_addons = 0
                num_non_system_addons = 0
                #### If you want to do this using DataFrames, size(array) gives
                #### you the length of an array-valued field.
                for addon in ping.active_addons[i]:
                    # could have None values, which are still included
                    if ping.active_addons[i][addon].is_system != True:
                        num_non_system_addons  += 1
                    num_total_addons += 1
                result = (ping.client_id, ((date, ping[facet]), {'total':num_total_addons, 
                                           'non_system': num_non_system_addons}))
                date_counts[i] = result
    else:
        date_counts[i] = (None, ((None,None), {}))
    return date_counts


#### For mean_addons_per_day, does each profile contribute only once to the mean?
#### It looks to me like you're using their most recent add-ons list?

# Yes, taking the most recent ping to account for multiple
# pings for the same client on the same day. 

def mean_addons_per_day(df, period=None, viz=False, figsize=(15, 10), marker='-',
                        channels='*'):
    '''
    Gets mean number of active_addons per profile over time for
    total addons and non_system addons
    
    df:     (Spark DataFrame) requires <submission_date> + <active_addons>
            fields from longitudinal dataset
    period: (tuple/list or None) time range i.e. ('2016-05-01', '2016-07-01')
    viz:    (boolean) should the result be plotted?
            if True, function returns result as list in memory (from collect()ing)
            if False, returns PythonRDD
    figsize:(tuple) (width, height) for plot, set at a good size for ipynb
    marker: marker used for plotting
    
    return: PythonRDD if viz else list
            (TimeStamp, {'total': <total_average>,
                         'non_system': <non_system_average>})
    
    '''
    # helper function for reduceByKey()
    def sum_by_day(x, y):
        total = x[0]['total'] + y[0]['total']
        non_system = x[0]['non_system'] + y[0]['non_system']
        n = x[1] + y[1]
        return ({'total':total, 'non_system':non_system}, n)
    
    # helper function for mapValues()
    def divide_and_format(x):
        '''
        Divides the 'total' field by n for an average
        and reorganizes output into one single dictionary
        in perparation for pandas
    
        To be passed to .map()
        '''
        total = x[1][0].get('total', 0) / x[1][1]
        non_system = x[1][0].get('non_system', 0) / x[1][1]
        time = x[0][0]
        channel = x[0][1]
        return {'total':total, 
            'non_system':non_system, 
            'n': x[1][1], 'time': time, 'channel': channel}
 

    # get total sum by day
    daily_addon_count = df.flatMap(lambda x: addons_per_day(x, period=period)) \
                        .reduceByKey(lambda x, y: x if x[0][0] > y[0][0] else y) \
                        .map(lambda x: x[1]) 
    
    # get mean by day
    daily_addon_mean = daily_addon_count\
                         .filter(lambda x: x[0] is not None)\
                         .mapValues(lambda v: (v, 1))\
                         .reduceByKey(lambda x, y: sum_by_day(x, y)) \
                         .map(lambda x: divide_and_format(x))
                    
    # Plotting (returns result in memory if viz=True)
    if viz:
        x = pd.DataFrame(daily_addon_mean.collect())
        for channel in Counter(x.channel):
            if channel is None:
                continue
            if channels == '*' or channel in channels:
                d=x[x.channel == channel]
                d.plot(x='time', y=['non_system', 'total'], figsize=figsize)
                plt.title('Averge # addons over time\nchannel=' + channel + '  n=' + str(sum(d.n)))
                plt.xlabel('Time')
                plt.ylabel('Average # addons')
        
    return daily_addon_mean
