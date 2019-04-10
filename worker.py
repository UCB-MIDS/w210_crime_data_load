import numpy as np
import pandas as pd
import s3fs
import sys
import os
import pickle
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

def round_hour(dt):
    if (dt.hour >= 0) and (dt.hour <= 6):
        return datetime(dt.year, dt.month, dt.day, 6,0)
    elif (dt.hour > 6) and (dt.hour <= 11):
        return datetime(dt.year, dt.month, dt.day, 11,0)
    elif (dt.hour > 11) and (dt.hour <= 17):
        return datetime(dt.year, dt.month, dt.day, 17,0)
    else:
        return datetime(dt.year, dt.month, dt.day, 23,0)


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
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

print('[' + str(datetime.now()) + '] Processing and transforming data...')
sys.stdout.flush()
try:
    crimes = crimes.iloc[:, 3: ]
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
                             ,[crime_classes[1],crime_classes[1],crime_classes[1],crime_classes[1],
                               crime_classes[2],crime_classes[2],crime_classes[2],
                               crime_classes[3],crime_classes[3],
                               crime_classes[4],crime_classes[4],
                               crime_classes[5],
                               crime_classes[6],
                               crime_classes[7],
                               crime_classes[8],crime_classes[8],
                               crime_classes[9],crime_classes[9],
                               crime_classes[10],
                               crime_classes[11],
                               crime_classes[12],
                               crime_classes[13],
                               crime_classes[14],crime_classes[14],
                               crime_classes[15],
                               crime_classes[16],
                               crime_classes[17],
                               crime_classes[18],crime_classes[18],
                               crime_classes[19],crime_classes[19],crime_classes[19],
                               crime_classes[17],
                               crime_classes[5],
                               crime_classes[4]])

    print('[' + str(datetime.now()) + ']        * Filtering columns...')
    sys.stdout.flush()
    crimes = crimes[['Primary Type','Community Area']]
    print('[' + str(datetime.now()) + ']        * Removing problem lines...')
    sys.stdout.flush()
    crimes = crimes.dropna(axis=0,how='any')
    print('[' + str(datetime.now()) + ']        * Converting time format...')
    sys.stdout.flush()
    crimes.index = pd.to_datetime(crimes.index)
except Exception as e:
    print('[' + str(datetime.now()) + '] Error performing first part of transformations.')
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

# print('[' + str(datetime.now()) + '] Performing transformations for time series models...')
# sys.stdout.flush()
# try:
#     print('[' + str(datetime.now()) + ']        * Convert date and time to blocks of hours...')
#     sys.stdout.flush()
#     crimes_ts = crimes.reset_index()
#     crimes_ts['Date'] = crimes_ts['Date'].apply(round_hour)
#     print('[' + str(datetime.now()) + ']        * Consolidating data and filling empty rows...')
#     sys.stdout.flush()
#     crimes_ts = crimes_ts.groupby(['Date','Primary Type','Community Area'])\
#                          .size().unstack().fillna(0).stack().reset_index(name='counts')
#     print('[' + str(datetime.now()) + ']        * One-Hot Encoding categorical variables...')
#     sys.stdout.flush()
#     crimes_ts = pd.concat([crimes_ts,pd.get_dummies(crimes_ts['Primary Type'], prefix='primaryType')],axis=1)
#     print('[' + str(datetime.now()) + ']            - Primary Type complete')
#     sys.stdout.flush()
#     crimes_ts = pd.concat([crimes_ts,pd.get_dummies(crimes_ts['Community Area'], prefix='communityArea')],axis=1)
#     print('[' + str(datetime.now()) + ']            - Community Area complete')
#     sys.stdout.flush()
#     print('[' + str(datetime.now()) + ']        * Sorting data frame to group communities and crime types...')
#     sys.stdout.flush()
#     crimes_ts.sort_values(by=['Community Area', 'Primary Type', 'Date'], inplace=True)
#     print('[' + str(datetime.now()) + ']        * Dropping unused columns...')
#     sys.stdout.flush()
#     comms = crimes_ts['Community Area'].unique()
#     cts = crimes_ts['Primary Type'].unique()
#     crimes_ts.drop(columns=['Date','Primary Type','Community Area'], inplace=True)
#     print('[' + str(datetime.now()) + ']        * Sorting column order...')
#     sys.stdout.flush()
#     cols = crimes_ts.columns.tolist()[1:] + crimes_ts.columns.tolist()[:1]
#     crimes_ts = crimes_ts[cols]
#     print('[' + str(datetime.now()) + ']        * Setting dataframe to sparse format...')
#     sys.stdout.flush()
#     crimes_ts = crimes_ts.to_sparse(fill_value=0)
#     print('[' + str(datetime.now()) + ']        * Re-strutucturing data into time lag format...')
#     sys.stdout.flush()
#     lag = 4*7*16    # 16 weeks
#     val_size = 240  # 240 time periods per community per type of crime (60 days)
#     partials_train = []
#     partials_val = []
#     comm_count = 1
#     for comm in comms:
#         print('[' + str(datetime.now()) + ']            - Running community '+str(comm_count)+' of '+str(len(comms)))
#         sys.stdout.flush()
#         ct_count = 1
#         for ct in cts:
#             print('[' + str(datetime.now()) + ']                # Running primary type '+ct+' (' + str(ct_count) + ' of ' + str(len(cts))+ ')' )
#             sys.stdout.flush()
#             crimes_ts_pt = crimes_ts[((crimes_ts['communityArea_'+comm] == 1) & (crimes_ts['primaryType_'+ct] == 1))]
#             print('[' + str(datetime.now()) + ']                    > Dataset sliced' )
#             sys.stdout.flush()
#             columns = [crimes_ts_pt.shift(i) for i in range(1, lag+1)]
#             print('[' + str(datetime.now()) + ']                    > Lagged time columns created' )
#             sys.stdout.flush()
#             columns.append(crimes_ts_pt.iloc[:,-1:])
#             crimes_ts_pt = pd.concat(columns, axis=1)
#             print('[' + str(datetime.now()) + ']                    > Columns concatenated' )
#             sys.stdout.flush()
#             crimes_ts_pt = crimes_ts_pt[lag:]
#             print('[' + str(datetime.now()) + ']                    > Removed initial rows' )
#             sys.stdout.flush()
#             partials_train.append(crimes_ts_pt[val_size+1:])
#             partials_val.append(crimes_ts_pt[:-val_size])
#             print('[' + str(datetime.now()) + ']                    > Done' )
#             sys.stdout.flush()
#             ct_count = ct_count + 1
#         comm_count = comm_count+1
#     crimes_ts_train = pd.concat(partials_train,ignore_index=True)
#     crimes_ts_val = pd.concat(partials_val,ignore_index=True)
# except Exception as e:
#     print('[' + str(datetime.now()) + '] Error performing transformations for time-series.')
#     print(e)
#     print('[' + str(datetime.now()) + '] Aborting...')
#     sys.stdout.flush()
#     sys.exit(1)
#
# print('[' + str(datetime.now()) + '] Writing time-series datasets...')
# sys.stdout.flush()
# try:
#     #output = './data/ProcessedDataset.parquet'                      # This line to write to local disk
#     output_train = 's3://w210policedata/datasets/OneHotEncodedTSDatasetTRAIN.parquet' # This line to write to S3
#     output_val = 's3://w210policedata/datasets/OneHotEncodedTSDatasetVAL.parquet'
#     crimes_ts_train.to_parquet(output_train,index=False)
#     crimes_ts_val.to_parquet(output_val,index=False)
#     del columns
#     del crimes_ts
#     del crimes_ts_pt
#     del crimes_ts_train
#     del crimes_ts_val
#     del partials_train
#     del partials_val
# except:
#     print('[' + str(datetime.now()) + '] Error writing time-series output dataset: '+output)
#     print('[' + str(datetime.now()) + '] Aborting...')
#     sys.exit(1)

print('[' + str(datetime.now()) + '] Continuing data transformation for fixed-time models...')
sys.stdout.flush()
try:
    crimes = crimes.reset_index()
    print('[' + str(datetime.now()) + ']        * Creating new features from columns...')
    sys.stdout.flush()
    crimes['Weekday'] = crimes['Date'].dt.dayofweek
    crimes['Week of Year'] = crimes['Date'].dt.weekofyear
    crimes['Hour of the Day'] = crimes['Date'].dt.hour
    crimes['Hour of the Day'].replace([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23],
                                      ['DAWN','DAWN','DAWN','DAWN','DAWN','DAWN','DAWN',
                                       'MORNING','MORNING','MORNING','MORNING','MORNING',
                                       'AFTERNOON','AFTERNOON','AFTERNOON','AFTERNOON','AFTERNOON','AFTERNOON',
                                       'EVENING','EVENING','EVENING','EVENING','EVENING','EVENING'],
                                      inplace=True)
    crimes['Year'] = pd.to_datetime(crimes['Date']).dt.year
    print('[' + str(datetime.now()) + ']        * Consolidating data and filling empty rows...')
    sys.stdout.flush()
    crimes = crimes.groupby(['Year','Primary Type','Community Area','Weekday','Week of Year','Hour of the Day'])\
                   .size().unstack().fillna(0).stack().reset_index(name='counts')
except Exception as e:
    print('[' + str(datetime.now()) + '] Error performing second part of transformations.')
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

print('[' + str(datetime.now()) + '] Writing intermediate dataset...')
sys.stdout.flush()
try:
    #output = './data/ProcessedDataset.parquet'                      # This line to write to local disk
    output = 's3://w210policedata/datasets/ProcessedDataset.parquet' # This line to write to S3
    crimes.to_parquet(output,index=False)
except:
    print('[' + str(datetime.now()) + '] Error writing intermediate output dataset: '+output)
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.exit(1)

try:
    print('[' + str(datetime.now()) + '] Continuing data processing and transformation...')
    sys.stdout.flush()
    print('[' + str(datetime.now()) + ']        * One-Hot Encoding categorical variables...')
    sys.stdout.flush()
    crimes = pd.concat([crimes,pd.get_dummies(crimes['Primary Type'], prefix='primaryType')],axis=1)
    print('[' + str(datetime.now()) + ']            - Primary Type complete')
    sys.stdout.flush()
    crimes = pd.concat([crimes,pd.get_dummies(crimes['Community Area'], prefix='communityArea')],axis=1)
    print('[' + str(datetime.now()) + ']            - Community Area complete')
    sys.stdout.flush()
    crimes = pd.concat([crimes,pd.get_dummies(crimes['Weekday'], prefix='weekDay')],axis=1)
    print('[' + str(datetime.now()) + ']            - Weekday complete')
    sys.stdout.flush()
    crimes = pd.concat([crimes,pd.get_dummies(crimes['Week of Year'], prefix='weekYear')],axis=1)
    print('[' + str(datetime.now()) + ']            - Week of year complete')
    sys.stdout.flush()
    crimes = pd.concat([crimes,pd.get_dummies(crimes['Hour of the Day'], prefix='hourDay')],axis=1)
    print('[' + str(datetime.now()) + ']            - Hour of day complete')
    sys.stdout.flush()
    print('[' + str(datetime.now()) + ']        * Dropping unused columns...')
    sys.stdout.flush()
    crimes.drop(columns=['Year','Primary Type','Community Area','Weekday','Week of Year','Hour of the Day'], inplace=True)
except Exception as e:
    print('[' + str(datetime.now()) + '] Error performing second part of transformations.')
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

print('[' + str(datetime.now()) + '] Writing one-hot encoded dataset...')
sys.stdout.flush()
try:
    #output = './data/OneHotEncodedDataset.parquet'                      # This line to write to local disk
    output = 's3://w210policedata/datasets/OneHotEncodedDataset.parquet' # This line to write to S3
    crimes.to_parquet(output,index=False)
except:
    print('[' + str(datetime.now()) + '] Error writing output dataset: '+output)
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.exit(1)

print('[' + str(datetime.now()) + '] Building available features file...')
sys.stdout.flush()
features = [
                {'feature': 'Community Area', 'column': 'communityArea', 'onehot-encoded': True, 'ethnically_biased': True, 'optional': True},
                {'feature': 'Crime Type', 'column': 'primaryType', 'onehot-encoded': True, 'ethnically_biased': False, 'optional': False},
                {'feature': 'Day of the Week', 'column': 'weekDay', 'onehot-encoded': True, 'ethnically_biased': False, 'optional': False},
                {'feature': 'Week of the Year', 'column': 'weekYear', 'onehot-encoded': True, 'ethnically_biased': False, 'optional': False},
                {'feature': 'Period of the Day', 'column': 'hourDay', 'onehot-encoded': True, 'ethnically_biased': False, 'optional': False}
           ]

print('[' + str(datetime.now()) + '] Writing available features file...')
sys.stdout.flush()
try:
    #output = './data/OneHotEncodedDataset.parquet'                      # This line to write to local disk
    output = 'w210policedata/datasets/AvailableFeatures.pickle' # This line to write to S3
    temp_file = tempfile.NamedTemporaryFile(delete=True)
    with s3.open(temp_file, "wb") as json_file:
        pickle.dump(features, json_file, protocol=pickle.HIGHEST_PROTOCOL)
        json_file.close()
    s3.put(temp_file.name,output)
    temp_file.close()
except:
    print('[' + str(datetime.now()) + '] Error writing features file: '+output)
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.exit(1)

print('[' + str(datetime.now()) + '] Finished!')
sys.exit(0)
