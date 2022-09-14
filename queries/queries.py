import sqlite3
import pandas as pd

tidalAgentDatabase = sqlite3.connect('/Users/amith/Downloads/tidal-agent.db')
orchJobs = pd.read_sql_query("select * from orch_jobs", tidalAgentDatabase)
hiveQueries = pd.read_sql_query("select *  from hive_queries", tidalAgentDatabase)


def getDurations():
    durations = pd.merge(orchJobs[['run_id', 'job_name']], hiveQueries[['run_id', 'duration']], on='run_id')
    durations.to_csv('../csvfiles/durations.csv')
    print("fetched durations")


def getStatistics():
    hiveQueries['duration'] = hiveQueries.duration.apply(convertToMillis)
    mergedData = pd.merge(orchJobs[['run_id', 'job_name']], hiveQueries[['run_id', 'query_id', 'duration']],
                          on='run_id')
    stats = mergedData.groupby('run_id').duration.agg(['max', 'min', 'mean', 'median'])
    queriesCount = mergedData.groupby('run_id').query_id.agg(['count'])
    result = pd.merge(pd.merge(orchJobs[['run_id', 'job_name']], stats, on='run_id'), queriesCount, on='run_id')
    result['max'] = result['max'].apply(convertToDays)
    result['min'] = result['min'].apply(convertToDays)
    result['mean'] = result['mean'].apply(convertToDays)
    result['median'] = result['median'].apply(convertToDays)
    result.to_csv('../csvfiles/statistics.csv')
    print("evaluated statistics")


def convertToMillis(duration):
    daysSplit = duration.split(' days ')
    days = int(daysSplit[0])
    time = daysSplit[1]
    timeSplit = time.split(':')
    hours = int(timeSplit[0])
    minutes = int(timeSplit[1])
    seconds = float(timeSplit[2])
    return seconds * 1000 + minutes * 60 * 1000 + hours * 60 * 60 * 1000 + days * 24 * 60 * 60 * 1000


def convertToDays(millis):
    days = int(millis / (1000 * 60 * 60 * 24))
    hours = int((millis / (1000 * 60 * 60)) % 24)
    minutes = int((millis / (1000 * 60)) % 60)
    seconds = round((millis / 1000) % 60, 6)
    return str(days) + ' days ' + convertToString(hours) + ':' + convertToString(minutes) + ':' + convertToString(
        seconds)


def convertToString(time):
    if time < 10:
        return '0' + str(time)
    return str(time)
