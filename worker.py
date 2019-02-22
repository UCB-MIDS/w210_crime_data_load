import numpy as np
import pandas as pd
import s3fs
import sys
import os
from datetime import datetime

# THEFT, BURGLARY, MOTOR VEHICLE THEFT, ROBBERY                 THEFT	                          1
# BATTERY, CRIM SEXUAL ASSAULT, SEX OFFENSE	                    SEXUAL ASSAULT	                  2
# NARCOTICS, OTHER NARCOTIC VIOLATION	                        NARCOTICS	                      3
# ASSAULT, INTIMIDATION	                                        ASSAULT	                          4
# OTHER OFFENSE	                                                OTHER OFFENSE	                  5
# DECEPTIVE PRACTICE	                                        DECEPTIVE PRACTICE	              6
# CRIMINAL TRESPASS	                                            CRIMINAL TRESPASS	              7
# WEAPONS VIOLATION, CONCEALED CARRY LICENSE VIOLATION	        WEAPONS VIOLATION	              8
# PUBLIC INDECENCY, PUBLIC PEACE VIOLATION	                    PUBLIC INDECENCY	              9
# OFFENSE INVOLVING CHILDREN	                                OFFENSE INVOLVING CHILDREN	      10
# PROSTITUTION	                                                PROSTITUTION	                  11
# INTERFERENCE WITH PUBLIC OFFICER	                            INTERFERENCE WITH PUBLIC OFFICER  12
# HOMICIDE	                                                    HOMICIDE	                      13
# ARSON, CRIMINAL DAMAGE	                                    ARSON	                          14
# GAMBLING	                                                    GAMBLING	                      15
# LIQUOR LAW VIOLATION	                                        LIQUOR LAW VIOLATION	          16
# KIDNAPPING	                                                KIDNAPPING	                      17
# STALKING, OBSCENITY	                                        STALKING	                      18
# NON - CRIMINAL, NON-CRIMINAL (SUBJECT SPECIFIED)	            NON - CRIMINAL	                  19
# HUMAN TRAFFICKING	                                            HUMAN TRAFFICKING	              20
# RITUALISM                                                     RITUALISM                         21
# DOMESTIC VIOLENCE                                             DOMESTIC VIOLENCE                 22

crime_classes = {1:'THEFT', 2:'SEXUAL ASSAULT', 3:'NARCOTICS', 4:'ASSAULT', 5:'OTHER OFFENSE', 6:'DECEPTIVE PRACTICE',
                 7:'CRIMINAL TRESPASS', 8:'WEAPONS VIOLATION', 9:'PUBLIC INDECENCY', 10:'OFFENSE INVOLVING CHILDREN',
                 11:'PROSTITUTION', 12:'INTERFERENCE WITH PUBLIC OFFICER', 13:'HOMICIDE', 14:'ARSON', 15:'GAMBLING',
                 16:'LIQUOR LAW VIOLATION', 17:'KIDNAPPING', 18:'STALKING', 19:'NON - CRIMINAL', 20:'HUMAN TRAFFICKING',
                 21:'RITUALISM',22:'DOMESTIC VIOLENCE'}

print('[' + str(datetime.now()) + '] Reading Crimes dataset...')
sys.stdout.flush()
s3fs.S3FileSystem.read_timeout = 5184000  # one day
s3fs.S3FileSystem.connect_timeout = 5184000  # one day
try:
    #file = './data/Crimes_-_2001_to_present.csv'                       # This line to read from local disk
    #file = 's3://w210policedata/datasets/Crimes_test.csv'              # This line to read quick test file from S3
    file = 's3://w210policedata/datasets/Crimes_-_2001_to_present.csv'  # This line to read from S3
    crimes = pd.read_csv(file,sep=',', error_bad_lines=False, index_col='Date', dtype='unicode')
except Exception as e:
    print('[' + str(datetime.now()) + '] Error reading input dataset: '+file)
    print('[' + str(datetime.now()) + '] Error message: '+str(e))
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

print('[' + str(datetime.now()) + '] Processing and transforming data...')
sys.stdout.flush()
crimes = crimes.iloc[:, 3: ]
crimes.index = pd.to_datetime(crimes.index)
print('[' + str(datetime.now()) + ']        * Grouping similar crime types...')
sys.stdout.flush()
crimes = crimes.replace(['THEFT', 'BURGLARY', 'MOTOR VEHICLE THEFT', 'ROBBERY' ,'BATTERY', 'CRIM SEXUAL ASSAULT',
                         'SEX OFFENSE' , 'NARCOTICS','OTHER NARCOTIC VIOLATION' , 'ASSAULT', 'INTIMIDATION' ,
                         'OTHER OFFENSE' , 'DECEPTIVE PRACTICE' , 'CRIMINAL TRESPASS' , 'WEAPONS VIOLATION' ,
                         'CONCEALED CARRY LICENSE VIOLATION','PUBLIC INDECENCY', 'PUBLIC PEACE VIOLATION',
                         'OFFENSE INVOLVING CHILDREN','PROSTITUTION','INTERFERENCE WITH PUBLIC OFFICER','HOMICIDE',
                         'ARSON', 'CRIMINAL DAMAGE','GAMBLING','LIQUOR LAW VIOLATION','KIDNAPPING','STALKING',
                         'OBSCENITY','NON - CRIMINAL','NON-CRIMINAL', 'NON-CRIMINAL (SUBJECT SPECIFIED)','HUMAN TRAFFICKING',
                         'RITUALISM','DOMESTIC VIOLENCE']
                         ,[1,1,1,1,2,2,2,3,3,4,4,5,6,7,8,8,9,9,10,11,12,13,14,14,15,16,17,18,18,19,19,19,20,21,22])

print('[' + str(datetime.now()) + ']        * Filtering columns...')
sys.stdout.flush()
crimes = crimes[['Primary Type','Beat']]
print('[' + str(datetime.now()) + ']        * Removing problem lines...')
sys.stdout.flush()
crimes = crimes.dropna(axis=0,how='any')
print('[' + str(datetime.now()) + ']        * Converting time format...')
sys.stdout.flush()
#crimes.index = pd.to_datetime(crimes.index)
crimes = crimes.reset_index()
print('[' + str(datetime.now()) + ']        * Creating new features from columns...')
sys.stdout.flush()
crimes['Weekday'] = crimes['Date'].dt.dayofweek
crimes['Week of Year'] = crimes['Date'].dt.weekofyear
crimes['Hour of the Day'] = crimes['Date'].dt.hour
crimes['Hour of the Day'].replace([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23],
                                  [0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,0],
                                  inplace=True)

print('[' + str(datetime.now()) + '] Writing intermediate dataset...')
sys.stdout.flush()
try:
    #output = './data/ProcessedDataset.csv'                      # This line to write to local disk
    output = 's3://w210policedata/datasets/ProcessedDataset.csv' # This line to write to S3
    crimes.to_csv(output,index=False)
except:
    print('[' + str(datetime.now()) + '] Error writing output dataset: '+output)
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.exit(1)

print('[' + str(datetime.now()) + '] Continuing data processing and transformation...')
sys.stdout.flush()
crimes['Year'] = pd.to_datetime(crimes['Date']).dt.year
print('[' + str(datetime.now()) + ']        * Consolidating data and filling empty rows...')
sys.stdout.flush()
crimes = crimes.groupby(['Year','Primary Type','Beat','Weekday','Week of Year','Hour of the Day'])\
               .size().unstack().fillna(0).stack().reset_index(name='counts')
print('[' + str(datetime.now()) + ']        * One-Hot Encoding categorical variables...')
sys.stdout.flush()
crimes = pd.concat([crimes,pd.get_dummies(crimes['Primary Type'], prefix='type')],axis=1)
print('[' + str(datetime.now()) + ']            - Primary Type complete')
sys.stdout.flush()
crimes = pd.concat([crimes,pd.get_dummies(crimes['Beat'], prefix='beat')],axis=1)
print('[' + str(datetime.now()) + ']            - Beat complete')
sys.stdout.flush()
crimes = pd.concat([crimes,pd.get_dummies(crimes['Weekday'], prefix='weekday')],axis=1)
print('[' + str(datetime.now()) + ']            - Weekday complete')
sys.stdout.flush()
crimes = pd.concat([crimes,pd.get_dummies(crimes['Week of Year'], prefix='weekyear')],axis=1)
print('[' + str(datetime.now()) + ']            - Week of year complete')
sys.stdout.flush()
crimes = pd.concat([crimes,pd.get_dummies(crimes['Hour of the Day'], prefix='hourday')],axis=1)
print('[' + str(datetime.now()) + ']             - Hour of day complete')
sys.stdout.flush()
print('[' + str(datetime.now()) + ']        * Dropping unused columns...')
sys.stdout.flush()
crimes.drop(columns=['Year','Primary Type','Beat','Weekday','Week of Year','Hour of the Day'], inplace=True)

print('[' + str(datetime.now()) + '] Writing one-hot encoded dataset...')
sys.stdout.flush()
try:
    #output = './data/OneHotEncodedDataset.csv'                      # This line to write to local disk
    output = 's3://w210policedata/datasets/OneHotEncodedDataset.csv' # This line to write to S3
    crimes.to_csv(output,index=False)
except:
    print('[' + str(datetime.now()) + '] Error writing output dataset: '+output)
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.exit(1)

print('[' + str(datetime.now()) + '] Finished!')
sys.exit(0)
