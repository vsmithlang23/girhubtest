import pandas as pd
import glob, csv, os
from functools import reduce
 
geo1_path = r"C:\Projects\UN\Indicators_4_22\New folder\geo1\*.csv"
geo2_path = r"C:\Projects\UN\Indicators_4_22\New folder\geo2\*.csv"
natl_path = r"C:\Projects\UN\Indicators_4_22\New folder\natl\*.csv"
 
dfs = []
geo1_dfs = []
geo2_dfs = []
natl_dfs = []
frame_cn = ['country', 'geolev1', 'geolev2', 'status']
geolev1_cn = ['geolev1']
geolev2_cn = ['geolev2']
 
print("geo1")
for geo1_fname in glob.glob(geo1_path):
    cn = ['country', 'geolev1']
    df = pd.read_csv(geo1_fname, header=0)
    df.rename(columns={'LatestYearValue': os.path.splitext(os.path.basename(geo1_fname))[0]}, inplace=True)
    df.rename(columns={'LatestYear': os.path.splitext(os.path.basename(geo1_fname))[0] + '_year'}, inplace=True)
    cn.append(os.path.splitext(os.path.basename(geo1_fname))[0])
    cn.append(os.path.splitext(os.path.basename(geo1_fname))[0] + '_year')
    df.drop(df.columns.difference(cn), axis=1, inplace=True)
    df = df[cn]
    df['geolev1'] = df['geolev1'].astype(int)
    print(os.path.splitext(os.path.basename(geo1_fname))[0])
    geolev1_cn.append(os.path.splitext(os.path.basename(geo1_fname))[0])
    geolev1_cn.append(os.path.splitext(os.path.basename(geo1_fname))[0] + '_year')
    frame_cn.append(os.path.splitext(os.path.basename(geo1_fname))[0])
    frame_cn.append(os.path.splitext(os.path.basename(geo1_fname))[0] + '_year')
    geo1_dfs.append(df)
 
print("geo2")
for geo2_fname in glob.glob(geo2_path):
    cn = ['country', 'geolev2']
    df = pd.read_csv(geo2_fname, header=0)
    df.rename(columns={'LatestYearValue': os.path.splitext(os.path.basename(geo2_fname))[0]}, inplace=True)
    df.rename(columns={'LatestYear': os.path.splitext(os.path.basename(geo2_fname))[0] + '_year'}, inplace=True)
    cn.append(os.path.splitext(os.path.basename(geo2_fname))[0])
    cn.append(os.path.splitext(os.path.basename(geo2_fname))[0] + '_year')
    df.drop(df.columns.difference(cn), axis=1, inplace=True)
    df = df[cn]
    print(os.path.splitext(os.path.basename(geo2_fname))[0])
    df['geolev2'] = df['geolev2'].astype(int)
    geolev2_cn.append(os.path.splitext(os.path.basename(geo2_fname))[0])
    geolev2_cn.append(os.path.splitext(os.path.basename(geo2_fname))[0] + '_year')
    geo2_dfs.append(df)
 
print("natl")
for natl_fname in glob.glob(natl_path):
    cn = ['country']
    df = pd.read_csv(natl_fname, header=0)
    df.rename(columns={'LatestYearValue': os.path.splitext(os.path.basename(natl_fname))[0]}, inplace=True)
    df.rename(columns={'LatestYear': os.path.splitext(os.path.basename(natl_fname))[0] + '_year'}, inplace=True)
    cn.append(os.path.splitext(os.path.basename(natl_fname))[0])
    cn.append(os.path.splitext(os.path.basename(natl_fname))[0] + '_year')
    df.drop(df.columns.difference(cn), axis=1, inplace=True)
    df = df[cn]
    print(os.path.splitext(os.path.basename(natl_fname))[0])
    natl_dfs.append(df)
 
temp_geo1_cn = ['geolev1', 'country']
temp_geo1_frame = pd.concat(geo1_dfs, axis=0, ignore_index=True, sort=False)
temp_geo1_frame.drop(temp_geo1_frame.columns.difference(temp_geo1_cn), axis=1, inplace=True)
temp_geo1_frame.drop_duplicates(subset ="geolev1", keep="first", inplace = True)
temp_geo1_frame.reset_index(drop=True, inplace = True)
 
geo1_frame = reduce(lambda  left,right: pd.merge(left,right,on=['geolev1'], how='outer'), geo1_dfs)
geo1_frame.drop(geo1_frame.columns.difference(geolev1_cn), axis=1, inplace=True)
geo1_frame = pd.merge(geo1_frame, temp_geo1_frame, on='geolev1', how='left')
geo1_frame.insert(0, 'status', 'admin1')
geolev1_cn.insert(0, 'country')
geolev1_cn.insert(0, 'status')
geo1_frame = geo1_frame[geolev1_cn]
dfs.append(geo1_frame)
geo1_frame.to_csv(r'C:\Projects\UN\Indicators_4_22\New folder\geo1.csv', index=False)
 
temp_geo2_cn = ['geolev2', 'country']
temp_geo2_frame = pd.concat(geo2_dfs, axis=0, ignore_index=True, sort=False)
temp_geo2_frame.drop(temp_geo2_frame.columns.difference(temp_geo2_cn), axis=1, inplace=True)
temp_geo2_frame.drop_duplicates(subset ="geolev2", keep="first", inplace = True)
temp_geo2_frame.reset_index(drop=True, inplace = True)
 
geo2_frame = reduce(lambda  left,right: pd.merge(left,right,on=['geolev2'], how='outer'), geo2_dfs)
geo2_frame.drop(geo2_frame.columns.difference(geolev2_cn), axis=1, inplace=True)
geo2_frame = pd.merge(geo2_frame, temp_geo2_frame, on='geolev2', how='left')
geo2_frame.insert(0, 'status', 'admin2')
geolev2_cn.insert(0, 'country')
geolev2_cn.insert(0, 'status')
geo2_frame = geo2_frame[geolev2_cn]
dfs.append(geo2_frame)
geo2_frame.to_csv(r'C:\Projects\UN\Indicators_4_22\New folder\geo2.csv', index=False)
 
natl_frame = reduce(lambda  left,right: pd.merge(left,right,on=['country'], how='outer'), natl_dfs)
natl_frame.insert(0, 'status', 'admin0')
dfs.append(natl_frame)
natl_frame.to_csv(r'C:\Projects\UN\Indicators_4_22\New folder\natl.csv', index=False)
 
frame = pd.concat(dfs, axis=0, ignore_index=True)
frame = frame[frame_cn]
frame.to_csv(r'C:\Projects\UN\Indicators_4_22\New folder\all_data.csv', index=False)
 
print(geo1_frame)
print(geo2_frame)
print(natl_frame)
print(frame)
