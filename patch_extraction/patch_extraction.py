################################################################################
################################################################################
# Functionalities implemented:
# (1) Distribute sets of folders to different machines for parallel processing
# (2) Accesses one DG image from each folder one at a time and corresponding to
# every struct_loc within that image, it extracts one (64 x 64)px patch.
# (3) For every struct_loc, it also looks for a land use type in the pickle file
# that contains sturctLocs + landuse data per coordinate of building.
# (4) saves image with name containing foldername, imagename, coordinates of
# building, and landuse type
# ***** SLICING NEEDS TO BE CHECKED *********
################################################################################
################################################################################
import rasterio as ri
import pandas as pd
# import geopandas as gpd
import cv2
import glob
from shapely.geometry import Point, Polygon
import itertools
import rasterio
import sys
import pickle
import code

def read_and_extract(img, img_rast, x, y, img_name, out_path):
    print("**********************", flush=True)
    print("Received patch extraction job", flush=True)
    x_pix, y_pix = img_rast.index(x, y)
    print("Extracted pixel locations based on lat lon", flush=True)
    print("Image Name: {}".format(img_name), flush=True)
    try:
        sliced_img = img[x_pix-32:x_pix+32, y_pix-32:y_pix+32, :]
        print("Sliced the image", flush=True)
        cv2.imwrite(out_path + img_name + ".png", sliced_img)
        print("Saved image with name {}".format(img_name), flush=True)
    except:
        print("Cannot be sliced.", flush=True)
    return None

def landcover_name_correction(ln):
    print("Received land cover type for name correction.")
    if ln == "Mosaic cropland (>50%) / natural vegetation (tree/shrub/herbaceous cover) (<50%)’":
        return "Mosaic cropland (>50%) or natural vegetation (tree or shrub or herbaceous cover) (<50%)’"
    elif ln == "Mosaic tree and shrub (>50%) / herbaceous cover (<50%)’":
        return "Mosaic tree and shrub (>50%) or herbaceous cover (<50%)’"
    elif ln == "Mosaic natural vegetation (tree/shrub/herbaceous cover) (>50%) / cropland (<50%)’":
        return "Mosaic natural vegetation (tree or shrub or herbaceous cover) (>50%) or cropland (<50%)’"
    elif ln == "Mosaic herbaceous cover (>50%) / tree and shrub (<50%)’":
        return "Mosaic herbaceous cover (>50%) or tree and shrub (<50%)’"
    elif ln == "Shrub or herbaceous cover flooded fresh/saline/brakish water’":
        return "Shrub or herbaceous cover flooded fresh or saline or brakish water’"
    elif ln == "Sparse vegetation (tree/shrub/herbaceous cover) (<15%)’":
        return "Sparse vegetation (tree or shrub or herbaceous cover) (<15%)’"
    else:
        return ln

def save_patches(slice_no, slice_length, pickled_files_path, dg_path, struct_locs_landuse_path, s_l_file, output_path):
    print("Reading building locations file appended with land cover information", flush=True)
    dll = pd.read_pickle(struct_locs_landuse_path + s_l_file)
    print("Read the locs + landuse file", flush=True)
    print("***********************************************************", flush=True)
    print("Figuring which folders to process based on slicing input", flush=True)
    list_of_folders = glob.glob(pickled_files_path + "*")
    total_length = len(list_of_folders)
    assert slice_length<=total_length, "Choose smaller slice length"
    print("Total number of available folders: {}".format(total_length), flush=True)
    folders_at_a_time = total_length//slice_length
    print("Number of folders to be read at a time: {}".format(folders_at_a_time), flush=True)
    if slice_no == slice_length-1:
        # maybe here +1 is not needed. ***** CHECK ********
        folders_to_process = list_of_folders[slice_no*folders_at_a_time : total_length+1]
    else:
        folders_to_process = list_of_folders[slice_no*folders_at_a_time : (slice_no+1)*folders_at_a_time]
    print("Folders to be processed on one machine: {}".format(folders_to_process), flush=True)
    print("Number of folders: {}".format(len(folders_to_process)), flush=True)
    for pck_file in folders_to_process:
        print("##############################################################################", flush=True)
        foldername = pck_file.split('/')[6].split('_pickle_file')[0] #check this before running
        print("Reading pickle file for folder {}".format(foldername), flush=True)
        df = pd.read_pickle(pck_file)
        print("Pickle file read!", flush=True)
        df['ct_array'] = df.struct_locs.apply(lambda x: len(x))
        print("Created struct_locs array length column", flush=True)
        df = df[(df.ct_array != 0)]
        print("Filtered dataframe by removing image names without structures i.e. with count=0", flush=True)
        print("Checking if dataframe is empty after filteration", flush=True)
        if len(df)==0:
            print("Dataframe is empty. Moving to next folder.", flush=True)
            continue
        print("Dataframe is not empty. Continuing the process!", flush=True)
        df = df.groupby(['file_name']).struct_locs.apply(lambda x: itertools.chain(*x)).reset_index()
        print("Created iter objects for locations in every image", flush=True)
        print("###################################", flush=True)
        print("Folder Name = {}".format(foldername), flush=True)
        print("Total number of files = {}".format(df.file_name.nunique()), flush=True)
        print("###################################", flush=True)
        file_counter = 0
        for fname in df.file_name.unique():
            print("------------------------------------------------------------------", flush=True)
            print("{} - Processing satellite image: {}".format(file_counter, fname), flush=True)
            filename = fname.split("/")[1]
            sat_img_path = glob.glob(dg_path + foldername + "/" + filename)[0]
            print("Sat image path extracted", flush=True)
            sat_img = cv2.imread(sat_img_path)
            print("Cv2 read!", flush=True)
            sat_img_rast = rasterio.open(sat_img_path)
            print("rasterio read!", flush=True)
            db = df[(df.file_name == fname)]
            # An itertool chain object can only be used once and so
            # we save it in list instead and iterate through the list
            iter_list = list(db.struct_locs.values[0])
            print("Total buildings in image: {}".format(len(iter_list)), flush=True)
            print("Beginning patch extraction", flush=True)
            locs_counter = 0
            for p in iter_list:
                print("=================================", flush=True)
                x, y = p
                print("{} - Obtained (x,y) coordinates: {}".format(locs_counter, p), flush=True)
                try:
                    land_use = dll[(dll.gps_x==x) & (dll.gps_y==y)].land_use.values[0]
                    land_use = landcover_name_correction(land_use)
                    print("Extracted associated land use info: {}".format(land_use), flush=True)
                    img_name = foldername + "_z_" + filename.split('.')[0] + "_z_" + str(x) + "_z_" + str(y) + "_z_" + "build" + "_z_" + land_use
                    print("Sending locs for a point:{} for patch extraction".format(p), flush=True)
                    patch = read_and_extract(sat_img, sat_img_rast, x, y, img_name, output_path)
                except:
                    print("Couldn't extract associated land info for {} {} pair".format(x,y), flush=True)
                locs_counter = locs_counter + 1
            file_counter = file_counter + 1


slice_no = int(sys.argv[1])
slice_length = int(sys.argv[2])
# pickled_files_path = "/users/zealshah/Documents/deep_learning_project/pickled_outputs/"
# dg_path = "/users/zealshah/Documents/deep_learning_project/DG_new/"
# output_path = "/users/zealshah/Documents/deep_learning_project/outputs_patches/"
# struct_locs_landuse_path = "/users/zealshah/Documents/deep_learning_project/kenya_struct_locs/"
# struct_landuse_filename = "structLocs_landcover.pck"

pickled_files_path = "/home/zshah/DG_struct_locs/pickled_outputs/"
dg_path = "/home/jtaneja/data/DG/DG_new/"
output_path = "/mnt/nfs/eguide/projects/bldgpop/patches/buildings/"
struct_locs_landuse_path = "/home/zshah/DG_struct_locs/kenya_struct_locs/"
struct_landuse_filename = "structLocs_landcover.pck"

save_patches(slice_no, slice_length, pickled_files_path, dg_path, struct_locs_landuse_path, struct_landuse_filename, output_path)


# def test_slicing(slice_length, pickled_files_path):
#     print("Figuring which folders to process based on slicing input")
#     list_of_folders = glob.glob(pickled_files_path + "*")
#     total_length = len(list_of_folders)
#     print("Total number of folders before slicing: {}".format(total_length))
#     folders_at_a_time = total_length//slice_length
#     print("Folders to be processed at once: {}".format(folders_at_a_time))
#     for slice_no in range(slice_length):
#         print("**************************************************")
#         print("SLICE NO: {}".format(slice_no))
#         if slice_no == slice_length-1:
#             print("Yes!!!")
#             folders_to_process = list_of_folders[slice_no*folders_at_a_time : total_length + 1]
#         else:
#             folders_to_process = list_of_folders[slice_no*folders_at_a_time : (slice_no+1)*folders_at_a_time]
#         print("Folders to be processed: {}".format(folders_to_process))
#         print("Number of folders: {}".format(len(folders_to_process)))


