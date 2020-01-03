# ------------------------------------------------------------------
# ------------------------------------------------------------------

# DESCRIPTION
'''
 This python script is used to create a summary report of client usage
 based on the provided TSV log files.

 Specifically, this script takes a collection of TSV logs (one per day)
 for the Geocoder, combines into one file, runs calculations with Pandas,
 saves findings into a dataFrame and creates a CSV summary report containing
 the following.

 1. Unique user ids
 2. Full name of the user
 3. Organization name
 4. Count per user of jobs submitted
 5. Total number of addresses geocoded per user
 6. Average number of addresses per job

Instructions:

 1. Start menu -> Run -> Type 'cmd'
 2. Navigate to the folder where this script is located.

        python Log_Report_Generator.py <API key>
                                   <filepath to the csv lookup table>
                                   <filepath to the zip file
                                    containing the logs>
                                   <startDate>
                                         *written as '2019-08-25'
                                   <endDate>
                                         *written as '2019-10-25'
Assumptions:

- You are providing a file in ZIP format containing all logs.
- The zip file is named the same (see code below), thus creating
a folder by the same name. Within that folder is one subfolder called
'jobs', then within that are all the logs in TSV format.
- The 'WHEN_CREATED' field in the log files use the following
format 'YYYY-MM-DD 00:00:00.000'

'''
# -------------------------------------------------------------------
# -------------------------------------------------------------------

# IMPORT MODULES

import csv
import datetime
import glob
import numpy as np
import os
import pandas as pd
import shutil
import sys
import zipfile

# -------------------------------------------------------------------
# -------------------------------------------------------------------

# VARIABLE DEFINITIONS

# The API key. See password manager for value
apiKey = sys.argv[1]

# The path to the csv client lookup table (including file name and extension)
csvLookUpTable = sys.argv[2]

# The path to the zip file containing all the TSV log files
zipLogFiles = sys.argv[3]


# User defines how far back in the logs to go for calculations
# NOTE: single digit months must have a leading zero!

startDate = sys.argv[4]
endDate = sys.argv[5]

# The folder where the zip file containing the TSV files is stored
zipLogFilesExtract = zipLogFiles.rsplit('\\', 1)[0] + '\logFiles'

# Subfolder path to log files
subFolder = zipLogFilesExtract + '\jobs'

# the merged file
mergedLogFile = zipLogFiles.rsplit('\\', 1)[0] + '\merged.tsv'

# the new csv file
csvLogFile = zipLogFiles.rsplit('\\', 1)[0] + '\mergedCSV.csv'

# the final Pandas report (an intermediate file overall)
finalReport = zipLogFiles.rsplit('\\', 1)[0] + r'\finalReport.csv'

# define a dataframe
csvLogFileContent = pd.DataFrame(columns=['ID',
                                          'BUSINESS_APPLICATION_NAME',
                                          'BATCH_JOB_ID', 'USER_ID',
                                          'WHEN_CREATED',
                                          'COMPLETED_TIMESTAMP',
                                          'NUM_SUBMITTED_REQUESTS',
                                          'FAILED_REQUEST_RANGE',
                                          'INPUT_DATA_CONTENT_TYPE',
                                          'RESULT_DATA_CONTENT_TYPE',
                                          ])

dfLookUpTable = pd.DataFrame(columns=['USER_ID', 'User_Name', 'Email', 'Org'])

dfFinalReport = pd.DataFrame(columns=['USER_ID', 'Total', 'Jobs'])


# Variable used to create files
directory = './'
extension = '.tsv'

# -------------------------------------------------------------------
# -------------------------------------------------------------------

# Step 1: Unzip the file containing the tsv file logs

print('Unzipping log package')
with zipfile.ZipFile(zipLogFiles, 'r') as zip_ref:
    zip_ref.extractall(zipLogFilesExtract)

# -------------------------------------------------------------------

# Step 2: Merge all TSVs into a single TSV
# (in cmd it is just->   copy *.tsv merged.tsv)

print('\nMerging log files into one file')
with open(mergedLogFile, 'w') as outfile:
    for fname in glob.glob(subFolder + '\\' + '*.tsv'):
        with open(fname, 'r') as readfile:
            outfile.write(readfile.read() + "\n\n")

# -------------------------------------------------------------------

# Step 3: Convert the TSV to a CSV.
# (smaller file size)

print('\nConverting log from tsv to csv')
tsv_file = mergedLogFile
csv_table = pd.read_table(tsv_file,sep='\t')
csv_table.to_csv(csvLogFile, index=False)

# -------------------------------------------------------------------

# Step 4: Read the csv into memory

# Read the file contents into a variable
print('\nReading the merged CSV log into a DataFrame')
csvLogFileContent = pd.read_csv(csvLogFile)

# -------------------------------------------------------------------

# Step 5: Integer data type

# Remove rows that don't have numbers in the
# 'NUM_SUBMITTED_REQUESTS' column. This removes all
# the headers in the file resulting from the log file merge
csvLogFileContent = csvLogFileContent[pd.to_numeric(csvLogFileContent
                                                    ['NUM_SUBMITTED_REQUESTS'],
                                                    errors='coerce').notnull()]

# Ensure that the 'NUM_SUBMITTED_REQUESTS' column is
# an integer for calculations
csvLogFileContent['NUM_SUBMITTED_REQUESTS'] = csvLogFileContent. \
                                              NUM_SUBMITTED_REQUESTS.astype(int)

# -------------------------------------------------------------------

# Step 6: Remove data beyond desired time range

print('\nThis is the range of dates in the log file')
# Oldest log file date
print(csvLogFileContent['WHEN_CREATED'][csvLogFileContent.index[0]])
# Most recent log file date
print(csvLogFileContent['WHEN_CREATED'][csvLogFileContent.index[-1]])

print('\nCreate a smaller dataframe using the user date range of: ')
print('\nFrom: ' + startDate + ' to ' + endDate)
csvLogFileContentTrimmed = csvLogFileContent.loc[csvLogFileContent['WHEN_CREATED']. \
                                                 str.split(' ').str[0] >= startDate]

csvLogFileContentTrimmed = csvLogFileContentTrimmed.loc[csvLogFileContent['WHEN_CREATED']. \
                                                 str.split(' ').str[0] <= endDate]
# print(csvLogFileContentTrimmed)

# -------------------------------------------------------------------

# Step 7: Job submissions, Average, Sum total addresses per user

# Calculate the number of job submissions per client
print('\nCalculate submissions per user')
seriesJobSubmissions = csvLogFileContentTrimmed['USER_ID'].value_counts()

# Calculate the average number of addresses per job by client
print('\nCalculate average addresses per job')
seriesAveragePerJob = csvLogFileContentTrimmed.groupby('USER_ID').mean() \
                      .NUM_SUBMITTED_REQUESTS

# Calculate the total number of addresses per client
print('\nCalculate total addresses processed per user')
seriesTotalAddresses = csvLogFileContentTrimmed.groupby('USER_ID').sum(). \
                       NUM_SUBMITTED_REQUESTS

print('\nNumber of unique users found in logs:')
print(len(seriesTotalAddresses))

# -------------------------------------------------------------------

# Step 8: Create a final report


print('\nMerge query results (series) into a single DataFrame')

dfTotalAddresses = seriesTotalAddresses.to_frame().reset_index()
dfTotalAddresses.columns = ['USER_ID', 'Total']

dfJobSubmissions = seriesJobSubmissions.to_frame().reset_index()
dfJobSubmissions.columns = ['USER_ID', 'Jobs']

dfAveragePerJob = seriesAveragePerJob.to_frame().reset_index()
dfAveragePerJob.columns = ['USER_ID', 'Avg']

# Merge the first two series (total jobs, total addresses) into
# one dataframe
dfFinalReport = dfTotalAddresses.merge(dfJobSubmissions)
# Merge the third series (average number per job) into the
# dataframe
dfFinalReport = dfFinalReport.merge(dfAveragePerJob.round(1))

# -------------------------------------------------------------------

# Step 9: Compare raw report (CSV) to look-up table (CSV)

# Read the lookup table into a dataFrame
dfLookUpTable = pd.read_csv(csvLookUpTable)

# merge with the finalReport dataFrame
# merge to ensure that all user_ids listed in the
# logs are included in the output. A standard merge for example
# would drop any that don't have a match in the lookup table.
# Using left will keep all unique users found in the log files
# and included matching attributes from the lookup table such
# as user name, email, organization
dfCPFUsage = dfFinalReport.merge(dfLookUpTable, how='left')

# -------------------------------------------------------------------

# Step 10: Export to CSV

# Export to CSV
dfCPFUsage.to_csv(finalReport, encoding='utf-8', index=False)

# -------------------------------------------------------------------

# Step 11: Clean-up logfiles and intermediate files.

print('\nDeleting intermediate files (including logs)')

try:
    os.remove(mergedLogFile)
    os.remove(csvLogFile)
    shutil.rmtree(zipLogFilesExtract)
except OSError as e:
    print ("Error: %s - %s." % (e.filename, e.strerror))

print('\nProcessing complete')
