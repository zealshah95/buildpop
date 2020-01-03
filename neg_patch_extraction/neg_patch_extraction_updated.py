import shapely
from shapely.geometry import Point, Polygon, box
from shapely.ops import cascaded_union
import pandas as pd
import time
# import geopandas as gpd
import cv2
import glob
import itertools
import rasterio
import sys
import random
import code

def getRadius(buffer):
   lat_degree = 110.54 * 1000
   lon_degree = 111.32 * 1000
   lat_radius = buffer / lat_degree
   lon_radius = buffer / lon_degree
   radius = max(lat_radius,lon_radius)
   return radius

def read_and_extract(img, img_rast, x, y, img_name, out_path):
    ######################################################
    # creates a 64 x 64 patch with given location at its
    # center
    ######################################################
    print("**********************")
    print("Received patch extraction job")

    x_pix, y_pix = img_rast.index(x, y)
    print("Extracted pixel locations based on lat lon")
    print("Image Name: {}".format(img_name))
    try:
        # 32 pixels on each side will create 64 x 64 pixel patches
        sliced_img = img[x_pix-32:x_pix+32, y_pix-32:y_pix+32, :]
        print("Sliced the image")
        cv2.imwrite(out_path + img_name + ".png", sliced_img)
        print("Saved image with name {}".format(img_name))
    except:
        print("Cannot be sliced.")
    return None

def generate_random_spatialpoints(number, polygon):
    #Source: https://gis.stackexchange.com/questions/207731/generating-random-coordinates-in-multipolygon-in-python
    list_of_points = []
    minx, miny, maxx, maxy = polygon.bounds
    counter = 0
    while counter < number:
        x = random.uniform(minx, maxx)
        y = random.uniform(miny, maxy)
        pnt = Point(x,y)
        if polygon.contains(pnt):
            list_of_points.append((x,y))
            counter += 1
    return list_of_points

def save_patches(slice_no, slice_length, pickled_files_path, dg_path, struct_locs_landuse_path, output_path, buff_radius):
    # print("Reading building locations file appended with land cover information", flush=True)
    # dll = pd.read_pickle(struct_locs_landuse_path + s_l_file)
    # print("Read the locs + landuse file", flush=True)
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
        foldername = pck_file.split('/')[6].split('_pickle_file')[0] #6 for laptop, 5 for gypsum?
        print("Reading pickle file for folder {}".format(foldername), flush=True)
        df = pd.read_pickle(pck_file)
        print("Pickle file read!", flush=True)
        # df['ct_array'] = df.struct_locs.apply(lambda x: len(x))
        # print("Created struct_locs array length column", flush=True)
        # df = df[(df.ct_array != 0)]
        # print("Filtered dataframe by removing image names without structures i.e. with count=0", flush=True)
        # print("Checking if dataframe is empty after filteration", flush=True)
        # if len(df)==0:
        #     print("Dataframe is empty. Moving to next folder.", flush=True)
        #     continue
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
            print('{} : Processing satellite image'.format(time.strftime('%m/%d/%Y %H:%M:%S',time.localtime())))
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
            # create a hollow box representing boundary of raster
            bound = sat_img_rast.bounds
            img_box = box(bound.left, bound.bottom, bound.right, bound.top)
            # reduce the boundary of the box in order to avoid
            # errors when the points are selected near the edge of the image
            # and the negative patch length crosses the image.
            # boundary buffer radius will be equal to radius of
            # patch (16m or 32pixels)
            boundary_buff = buff_radius/2
            img_box = box(bound.left+boundary_buff, bound.bottom+boundary_buff, bound.right-boundary_buff, bound.top-boundary_buff)
            print("image box cropped at the edges for buffering purposes")
            # if an image contains no buildings, then the complete image can
            # be used for extracting negative patches.
            # In that case, we won't need cascaded union
            if len(iter_list) !=0:
                print("Proceeding with cascaded union formation")
                # create point geoms for using in cascaded_union
                geom = []
                for k in iter_list:
                    if k != []: #avoids "not enough values to unpack" error
                        geom.append(Point(k[0],k[1]))
                    else:
                        None
                # geom = [Point(k[0],k[1]) for k in iter_list]
                # create buffers around each point
                buff = [x.buffer(buff_radius, cap_style=3) for x in geom]
                # create cascaded_union object
                casc_union = cascaded_union(buff)
                print("Cascaded union is ready")
                # extract portion of box that doesn't intersect with cascaded union
                # this shape should not contain buildings
                if casc_union.intersects(img_box) == True:
                    nobuild_poly = img_box.difference(casc_union)
            else:
                print("No buildings in image. Using complete image for extracting negative patches")
                nobuild_poly = img_box

            # create a list of random geo-spatial points that lie
            # Approx 1500 patches per image (127 folders x 64 images per folder)
            # will give us 12 million negative patches
            print('{} : Negative Poly is ready'.format(time.strftime('%m/%d/%Y %H:%M:%S',time.localtime())))
            print("Extracting 1500 random non-building points inside the image file")
            nobuild_list = generate_random_spatialpoints(1500, nobuild_poly)
            print('{} : Negative Points extracted'.format(time.strftime('%m/%d/%Y %H:%M:%S',time.localtime())))

            locs_counter = 0
            for p in nobuild_list:
                print("=================================", flush=True)
                x, y = p
                print("{} - Obtained (x,y) coordinates: {}".format(locs_counter, p), flush=True)
                img_name = foldername + "_z_" + filename.split('.')[0] + "_z_" + str(x) + "_z_" + str(y) + "_z_" +"nobuild"
                print("Sending locs for a point:{} for patch extraction".format(p), flush=True)
                patch = read_and_extract(sat_img, sat_img_rast, x, y, img_name, output_path)
                locs_counter = locs_counter + 1
            file_counter = file_counter + 1

slice_no = int(sys.argv[1])
slice_length = int(sys.argv[2])
dist_val = getRadius(32) #buffer of 32m radius around each point for cascaded union

# pickled_files_path = "/users/zealshah/Documents/deep_learning_project/pickled_outputs/"
# dg_path = "/users/zealshah/Documents/deep_learning_project/DG_new/"
# output_path = "/users/zealshah/Documents/deep_learning_project/outputs/"
# struct_locs_landuse_path = "/users/zealshah/Documents/deep_learning_project/land_use_data_processing/outputs/unified_locs_land_file"

pickled_files_path = "/home/zshah/DG_struct_locs/pickled_outputs/"
dg_path = "/home/jtaneja/data/DG/DG_new/"
output_path = "/mnt/nfs/eguide/projects/bldgpop/patches/no_buildings/"
struct_locs_landuse_path = "/home/zshah/DG_struct_locs/land_use_data_processing/outputs/"
struct_landuse_filename = "structLocs_landcover"

print('{} : Starting'.format(time.strftime('%m/%d/%Y %H:%M:%S',time.localtime())))
save_patches(slice_no, slice_length, pickled_files_path, dg_path, struct_locs_landuse_path, output_path, dist_val)
print('{} : Ending'.format(time.strftime('%m/%d/%Y %H:%M:%S',time.localtime())))