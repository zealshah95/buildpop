import sys,pandas as pd,georasters as gr,time

struct_locs_path = "/users/zealshah/Documents/deep_learning_project/kenya_struct_locs/"
land_use_path = "/users/zealshah/Documents/deep_learning_project/land_use_data_processing/land_use_data/"
out_path = "/users/zealshah/Documents/deep_learning_project/land_use_data_processing/outputs/"

# struct_locs_path = "/home/zshah/DG_struct_locs/kenya_struct_locs/"
# land_use_path = "/home/zshah/DG_struct_locs/land_use_data_processing/land_use_data/"
# out_path = "/home/zshah/DG_struct_locs/land_use_data_processing/outputs/"

print("************************Job starts here******************************")
landusesymbols = {float(line.split(',')[0]) : line.split(',')[1].rstrip().replace("'","") for line in open(land_use_path + 'landusesymbols2.csv','r').readlines()[1:]}

landuseclasses = {
    'cropland' : [10,11,12,20,30,40],
    'forest' : [50,60,61,62,70,71,72,80,81,82,90,100,110],
    'shrub and grassland' : [120,121,122,130,140],
    'sparse' : [150,151,152,153],
    'water' : [160,170,180,210],
    'urban' : [190],
    'bare' : [200,201,202],
    'ice' : [220]
}

# Create a dictionary numlookup that uses original land use names
numlookup = {}
for key in landuseclasses.keys():
    for item in landuseclasses[key]:
        numlookup[item] = str(landusesymbols[item])
print("Numlookup is ready.")

# extract slice_no and slice_length that are passed as arguments to the python file
slice_no = int(sys.argv[1])
slice_length = int(sys.argv[2])

print("Job number: {}".format(slice_no))
print("Reading NRECA data")
dbuild = pd.read_pickle(struct_locs_path + "structLocs2.pck")
# dbuild = dbuild.reset_index()
# dbuild = dbuild.rename(columns = {'index':'index_old'})
print("NRECA data has been loaded")

################ Slicing the dataset #######################
# l = len(dbuild)
# print("Length of original dataset: {}".format(l))
# data_length = l//slice_length #portion of data processed at a time
# if slice_no == slice_length-1:
#     dbuild = dbuild[data_length*slice_no : l]
# else:
#     dbuild = dbuild[data_length*slice_no : data_length*(slice_no+1)+1]
dbuild = dbuild[(dbuild.group == slice_no)][0:20]

print("NRECA data has been sliced")
print("Length of sliced dataset: {}".format(len(dbuild)))
print("Min Index Value: {} and Max Index Value: {}".format(dbuild.index.min(), dbuild.index.max()))
#Extract geokenya tif file
data = gr.from_file(land_use_path + 'geokenya.tif')
df = data.to_pandas()
print("Georaster read and converted to pandas")

######################################BUILDINGS##################################################
def get_landuse(db):
    print("########################")
    print("Extracting land use case")
    lon = db['gps_x']
    lat = db['gps_y']

    print("finding row and column of closest point")
    col = df.iloc[(df['x']-lon).abs().argsort()[:1],1].values[0]
    row = df.iloc[(df['y']-lat).abs().argsort()[:1],0].values[0]
    print("found row and column of closest point")

    # figure out where our point is in relation to the closest point
    # lat, lon of point is the top-left of grid square
    colFlag = False
    if len(df['x'][(df['col'] == col) & (df['row'] == row)].values) == 0:
        colFlag = True
        this_col = 0
    elif lon >= (df['x'][(df['col'] == col) & (df['row'] == row)].values[0]):
        this_col = col
        colFlag == False
    else:
        this_col = col-1
        colFlag == False

    rowFlag = False
    if len(df['y'][(df['col'] == col) & (df['row'] == row)].values) == 0:
        rowFlag = True
        this_row = 0
    elif lat >= (df['y'][(df['col'] == col) & (df['row'] == row)].values[0]):
        this_row = row-1
        rowFlag = False
    else:
        this_row = row
        rowFlag = False

    # deal with values that are not on our map
    if (this_row <= 1) or (this_col <= 1) or (rowFlag == True) or (colFlag == True):
        print("Deal with values not on our map!")
        this_row = float('nan')
        this_col = float('nan')
        this_value = float('nan')
        this_type = float('nan')
        print(','.join([str(lat),str(lon),'nan']))
    else:
        if len(df['value'][(df['col'] == this_col) & (df['row'] == this_row)].values) == 0:
            print("Check if the updated row and column values still lie on the map")
            this_row = float('nan')
            this_col = float('nan')
            this_value = float('nan')
            this_type = float('nan')
        else:
            print("Values on the map! Get the land use type")
            this_value = df['value'][(df['col'] == this_col) & (df['row'] == this_row)].values[0]
            this_type = numlookup[this_value]
        # print lat,lon,this_value,this_type
        print(','.join([str(lat),str(lon),str(this_type)]))
    return str(this_type)

print("Land use based processing begins here")
dbuild['land_use'] = dbuild.apply(get_landuse, axis=1)
print("Land use processed")
dbuild.to_pickle(out_path + "structLocs_landuse_{}".format(slice_no))
print("Pickle saved for slice number {}".format(slice_no))


# def sanity_check_slicing(slice_length, df):
#     print("Slice Length: {}".format(slice_length))
#     l = len(df)
#     print("Actual Dataset Length: {}".format(l))
#     data_length = l//slice_length
#     print("Length of individual slice: {}".format(data_length))
#     print("############################################")
#     for slice_no in range(slice_length):
#         print("Slice Number: {}".format(slice_no))
#         if slice_no == slice_length-1:
#             print("YES!!!!!!")
#             df1 = df[data_length*slice_no : l]
#             print("data slice: {} to {} and length: {}".format(df1.index.min(), df1.index.max(), len(df1)))
#         else:
#             df1 = df[data_length*slice_no : (data_length*(slice_no+1))+1]
#             print("data slice: {} to {} and length: {}".format(df1.index.min(), df1.index.max(), len(df1)))
#         del(df1)



