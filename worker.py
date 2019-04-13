import numpy as np
import pandas as pd
import s3fs
import sys
import os
import pickle
import tempfile
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

community_to_code = { 'Rogers Park':1,
                      'West Ridge':2,
                      'Uptown':3,
                      'Lincoln Square':4,
                      'North Center':5,
                      'Lake View':6,
                      'Lincoln Park':7,
                      'Near North Side':8,
                      'Edison Park':9,
                      'Norwood Park':10,
                      'Jefferson Park':11,
                      'Forest Glen':12,
                      'North Park':13,
                      'Albany Park':14,
                      'Portage Park':15,
                      'Irving Park':16,
                      'Dunning':17,
                      'Montclare':18,
                      'Belmont Cragin':19,
                      'Hermosa':20,
                      'Avondale':21,
                      'Logan Square':22,
                      'Humboldt Park':23,
                      'West Town':24,
                      'Austin':25,
                      'West Garfield Park':26,
                      'East Garfield Park':27,
                      'Near West Side':28,
                      'North Lawndale':29,
                      'South Lawndale':30,
                      'Lower West Side':31,
                      'The Loop':32,
                      'Near South Side':33,
                      'Armour Square':34,
                      'Douglas':35,
                      'Oakland':36,
                      'Fuller Park':37,
                      'Grand Boulevard':38,
                      'Kenwood':39,
                      'Washington Park':40,
                      'Hyde Park':41,
                      'Woodlawn':42,
                      'South Shore':43,
                      'Chatham':44,
                      'Avalon Park':45,
                      'South Chicago':46,
                      'Burnside':47,
                      'Calumet Heights':48,
                      'Roseland':49,
                      'Pullman':50,
                      'South Deering':51,
                      'East Side':52,
                      'West Pullman':53,
                      'Riverdale':54,
                      'Hegewisch':55,
                      'Garfield Ridge':56,
                      'Archer Heights':57,
                      'Brighton Park':58,
                      'McKinley Park':59,
                      'Bridgeport':60,
                      'New City':61,
                      'West Elsdon':62,
                      'Gage Park':63,
                      'Clearing':64,
                      'West Lawn':65,
                      'Chicago Lawn':66,
                      'West Englewood':67,
                      'Englewood':68,
                      'Greater Grand Crossing':69,
                      'Ashburn':70,
                      'Auburn Gresham':71,
                      'Beverly':72,
                      'Washington Heights':73,
                      'Mount Greenwood':74,
                      'Morgan Park':75,
                      'O\'Hare':76,
                      'Edgewater':77
                    }

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

try:
    print('[' + str(datetime.now()) + '] Reading CMAP dataset for additional features...')
    sys.stdout.flush()
    file = 's3://w210policedata/datasets/CMAP_dataset.csv'  # This line to read from S3
    cmap = pd.read_csv(file,sep=',', error_bad_lines=False, dtype='unicode')
except Exception as e:
    print('[' + str(datetime.now()) + '] Error reading CMAP dataset: '+file)
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

try:
    print('[' + str(datetime.now()) + '] Preparing and transforming CMAP dataset...')
    print('[' + str(datetime.now()) + ']        * Replacing community names with community codes...')
    sys.stdout.flush()
    cmap['communityArea'] = cmap['GEOG'].map(community_to_code)
    print('[' + str(datetime.now()) + ']        * Transforming columns...')
    sys.stdout.flush()
    cmap['socioEconomic_medianAge'] = pd.to_numeric(cmap['MED_AGE'])
    cmap['socioEconomic_medianIncome'] = pd.to_numeric(cmap['MEDINC'])
    cmap['socioEconomic_popInHouseholds'] = pd.to_numeric(cmap['POP_HH'])/pd.to_numeric(cmap['TOT_POP'])
    cmap['schooling_lessHighSchool'] = pd.to_numeric(cmap['LT_HS'])/pd.to_numeric(cmap['POP_25OV'])
    cmap['schooling_highSchool'] = pd.to_numeric(cmap['HS'])/pd.to_numeric(cmap['POP_25OV'])
    cmap['schooling_someCollege'] = pd.to_numeric(cmap['SOME_COLL'])/pd.to_numeric(cmap['POP_25OV'])
    cmap['schooling_Associate'] = pd.to_numeric(cmap['ASSOC'])/pd.to_numeric(cmap['POP_25OV'])
    cmap['schooling_Bachelor'] = pd.to_numeric(cmap['BACH'])/pd.to_numeric(cmap['POP_25OV'])
    cmap['schooling_Graduate'] = pd.to_numeric(cmap['GRAD_PROF'])/pd.to_numeric(cmap['POP_25OV'])
    cmap['housing_Occupied'] = pd.to_numeric(cmap['TOT_HH'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_Vacant'] = pd.to_numeric(cmap['VAC_HU'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_OwnerOccupied'] = pd.to_numeric(cmap['OWN_OCC_HU'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_RenterOccupied'] = pd.to_numeric(cmap['RENT_OCC_HU'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_SingleFamilyDetached'] = pd.to_numeric(cmap['HU_SNG_DET'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_SingleFamilyAttached'] = pd.to_numeric(cmap['HU_SNG_ATT'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_TwoUnits'] = pd.to_numeric(cmap['HU_2UN'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_ThreeOrFourUnits'] = pd.to_numeric(cmap['HU_3_4UN'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_FiveOrMoreUnits'] = pd.to_numeric(cmap['HU_GT_5UN'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_medianNumberRooms'] = pd.to_numeric(cmap['MED_ROOMS'])
    cmap['housing_Later2000'] = pd.to_numeric(cmap['HA_AFT2000'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_1970to1999'] = pd.to_numeric(cmap['HA_70_00'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_1940to1969'] = pd.to_numeric(cmap['HA_40_70'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_Before1940'] = pd.to_numeric(cmap['HA_BEF1940'])/pd.to_numeric(cmap['HU_TOT'])
    cmap['housing_medianHouseAge'] = pd.to_numeric(cmap['MED_HA'])
    cmap['socioEconomic_inLaborForce'] = pd.to_numeric(cmap['IN_LBFRC'])/pd.to_numeric(cmap['POP_16OV'])
    cmap['socioEconomic_employed'] = pd.to_numeric(cmap['EMP'])/pd.to_numeric(cmap['IN_LBFRC'])
    cmap['socioEconomic_unemployed'] = pd.to_numeric(cmap['UNEMP'])/pd.to_numeric(cmap['IN_LBFRC'])
    cmap['socioEconomic_notInLaborForce'] = pd.to_numeric(cmap['NOT_IN_LBFRC'])/pd.to_numeric(cmap['POP_16OV'])
    cmap['commute_carAlone'] = pd.to_numeric(cmap['DROVE_AL'])/pd.to_numeric(cmap['TOT_COMM'])
    cmap['commute_carpool'] = pd.to_numeric(cmap['CARPOOL'])/pd.to_numeric(cmap['TOT_COMM'])
    cmap['commute_transit'] = pd.to_numeric(cmap['TRANSIT'])/pd.to_numeric(cmap['TOT_COMM'])
    cmap['commute_walkOrBike'] = pd.to_numeric(cmap['WALK_BIKE'])/pd.to_numeric(cmap['TOT_COMM'])
    cmap['commute_other'] = pd.to_numeric(cmap['COMM_OTHER'])/pd.to_numeric(cmap['TOT_COMM'])
    cmap['commute_averageVehicleMilesTravelled'] = pd.to_numeric(cmap['AVG_VMT'])
    cmap['socioEconomic_noVehiclesAvailable'] = pd.to_numeric(cmap['NO_VEH'])/pd.to_numeric(cmap['TOT_HH'])
    cmap['socioEconomic_oneVehicleAvailable'] = pd.to_numeric(cmap['ONE_VEH'])/pd.to_numeric(cmap['TOT_HH'])
    cmap['socioEconomic_twoVehiclesAvailable'] = pd.to_numeric(cmap['TWO_VEH'])/pd.to_numeric(cmap['TOT_HH'])
    cmap['socioEconomic_threeOrMoreVehiclesAvailable'] = pd.to_numeric(cmap['THREEOM_VEH'])/pd.to_numeric(cmap['TOT_HH'])
    cmap['lifeQuality_accessibleParkAcreage'] = pd.to_numeric(cmap['OPEN_SPACE_PER_1000'])
    cmap['landUse_singleFamilyResidential'] = pd.to_numeric(cmap['Sfperc'])
    cmap['landUse_multiFamilyResidential'] = pd.to_numeric(cmap['Mfperc'])
    cmap['landUse_commercial'] = pd.to_numeric(cmap['COMMperc'])
    cmap['landUse_industrial'] = pd.to_numeric(cmap['INDperc'])
    cmap['landUse_institutional'] = pd.to_numeric(cmap['INSTperc'])
    cmap['landUse_mixedUse'] = pd.to_numeric(cmap['MIXperc'])
    cmap['landUse_transportation'] = pd.to_numeric(cmap['TRANSperc'])
    cmap['landUse_agricultural'] = pd.to_numeric(cmap['Agperc'])
    cmap['landUse_openSpace'] = pd.to_numeric(cmap['OPENperc'])
    cmap['landUse_vacant'] = pd.to_numeric(cmap['VACperc'])
    print('[' + str(datetime.now()) + ']        * Filtering columns...')
    sys.stdout.flush()
    regex="(communityArea)|(socioEconomic_)|(schooling_)|(housing_)|(commute_)|(lifeQuality_)|(landUse_)"
    cmap = cmap.filter(regex=regex,axis=1)
    print('[' + str(datetime.now()) + ']        * Filling in NAs...')
    sys.stdout.flush()
    cmap.fillna(0, inplace=True)
except Exception as e:
    print('[' + str(datetime.now()) + '] Error transforming CMAP dataset.')
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

try:
    print('[' + str(datetime.now()) + '] Writing additional features dataset...')
    sys.stdout.flush()
    output = 's3://w210policedata/datasets/AdditionalFeatures.parquet' # This line to write to S3
    cmap.to_parquet(output,index=False)
except Exception as e:
    print('[' + str(datetime.now()) + '] Error writing additional features dataset.')
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.stdout.flush()
    sys.exit(1)

try:
    print('[' + str(datetime.now()) + '] Combining Additional Features and Crimes datasets...')
    sys.stdout.flush()
    crimes = pd.merge(crimes, cmap, on='communityArea')
except Exception as e:
    print('[' + str(datetime.now()) + '] Error combining additional features and crimes datasets.')
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
                {'feature': 'Period of the Day', 'column': 'hourDay', 'onehot-encoded': True, 'ethnically_biased': False, 'optional': False},
                {'feature': 'Socioeconomic Data', 'column': 'socioEconomic', 'onehot-encoded': False, 'ethnically_biased': False, 'optional': True},
                {'feature': 'Schooling', 'column': 'schooling', 'onehot-encoded': False, 'ethnically_biased': False, 'optional': True},
                {'feature': 'Housing', 'column': 'housing', 'onehot-encoded': False, 'ethnically_biased': False, 'optional': True},
                {'feature': 'Commute', 'column': 'commute', 'onehot-encoded': False, 'ethnically_biased': False, 'optional': True},
                {'feature': 'Quality of Life', 'column': 'lifeQuality', 'onehot-encoded': False, 'ethnically_biased': False, 'optional': True},
                {'feature': 'Land Use', 'column': 'landUse', 'onehot-encoded': False, 'ethnically_biased': False, 'optional': True}
           ]

print('[' + str(datetime.now()) + '] Writing available features file...')
sys.stdout.flush()
try:
    #output = './data/OneHotEncodedDataset.parquet'                      # This line to write to local disk
    output = 'w210policedata/datasets/AvailableFeatures.pickle' # This line to write to S3
    s3 = s3fs.S3FileSystem(anon=False)
    with s3.open(output, "wb") as json_file:
        pickle.dump(features, json_file, protocol=pickle.HIGHEST_PROTOCOL)
        json_file.close()
except:
    print('[' + str(datetime.now()) + '] Error writing features file: '+output)
    print('[' + str(datetime.now()) + '] Aborting...')
    sys.exit(1)

print('[' + str(datetime.now()) + '] Finished!')
sys.exit(0)
