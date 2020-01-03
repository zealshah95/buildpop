import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import os
# import code

def read_and_process_ground_truth(struct_locs_path):
    #############################################################
    # load the NRECA data points and convert
    # xy coordinates to point geometries
    #############################################################
    print("Reading NRECA Pickle File")
    ds = pd.read_pickle(struct_locs_path + 'structLocs.pck')
    print("Converting coordinates to point geometries")
    geometry = [Point(xy) for xy in zip(ds.gps_x,ds.gps_y)]
    ds = gpd.GeoDataFrame(ds, crs={'init' :'epsg:4326'}, geometry=geometry)
    print("Coordinates converted")
    return ds

def associate_struct_locs_to_DG(ds, DG_path, out_path):
    ################################################################
    # Create an array of folders that have already been pickled
    ################################################################
    print("Create a list of already processed folders")
    processed_files = [os.path.basename(f) for f in glob.glob(out_path + '*')]
    processed_files = [fp.split('_pickle_file')[0] for fp in processed_files]
    print("Gathered a list of all the folders already processed")
    ################################################################
    # Iterate through every shape file in the AOP folders of DG_new
    ################################################################
    for shape_file in glob.glob(DG_path + "/*/*.shp"):
        folder_name = shape_file.split('/')[6]
        # check if the folder has been processed in the previous runs
        if folder_name in processed_files:
            print("########################################################################")
            print("Folder already processed in the last run. Switching to a different folder.")
            continue
        print("########################################################################")
        print("New folder! Beginning the process for {}".format(folder_name))
        final_df = pd.DataFrame(columns={'file_name','catalog_id','acq_date','struct_locs'}) #one pickle per folder
        print("Created empty dataframe")
        print("Reading meta shape file: {}".format(shape_file))
        df = gpd.read_file(shape_file)
        df.ACQ_DATE = df.ACQ_DATE.apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d"))
        df = df[(df.ACQ_DATE.dt.year >= 2015)] #Filter out all images captured before 2015
        # print("Filtered meta shape file by dates")

        ####################################################################
        # for every image in the folder extract coordinates of points that
        # lie inside that image
        ####################################################################
        for img_name, cat_id, acq_date in zip(df.FILENAME, df.CATALOG_ID, df.ACQ_DATE):
            print("###################################################################")
            print("Reading image: name {}, catalog_id {}, acq_date {}".format(img_name, cat_id, acq_date))
            geo_poly = df[(df.FILENAME == img_name) & (df.CATALOG_ID == cat_id) & (df.ACQ_DATE == acq_date)]['geometry']
            geo_poly = geo_poly.values[0]
            print("Extracted image's geometry")
            try:
                struct_pts = ds[ds.geometry.within(geo_poly)][['geometry']]
                print("Got struct_locs within that image")
                struct_pts['xy_coords'] = struct_pts.geometry.apply(lambda a: a.coords[0])
                points = list(struct_pts.xy_coords.values)
                print("Formed a list of struct_locs coordinates")
                final_df = final_df.append({'file_name':img_name, 'catalog_id': cat_id, 'acq_date':acq_date, 'struct_locs':points}, ignore_index=True)
                print("Dataframe appended")
            except:
                print("Error extracting struct_locs within or appending the dataframe")
                final_df = final_df.append({'file_name':img_name, 'catalog_id': cat_id, 'acq_date':acq_date, 'struct_locs':"error_encountered"}, ignore_index=True)
                print("Dataframe appended with Error placeholder")

        print("Saving to Pickle for {}".format(folder_name))
        final_df.to_pickle(out_path+'{}_pickle_file'.format(folder_name))
        print("Pickling done")
        del(final_df)
        print("Deleted dataframe")

    print("All SET!")
    # code.interact(local = locals())
    return None

if __name__ == '__main__':
    DG_path = "/users/zealshah/Documents/deep_learning_project/DG_new/"
    struct_locs_path = "/users/zealshah/Documents/deep_learning_project/kenya_struct_locs/"
    out_path = "/users/zealshah/Documents/deep_learning_project/outputs/"

    # DG_path = "/home/jtaneja/data/DG/DG_new/"
    # struct_locs_path = "/home/zshah/DG_struct_locs/kenya_struct_locs/"
    # out_path = "/home/zshah/DG_struct_locs/pickled_outputs/"

    d_ground = read_and_process_ground_truth(struct_locs_path)
    associate_struct_locs_to_DG(d_ground, DG_path, out_path)
