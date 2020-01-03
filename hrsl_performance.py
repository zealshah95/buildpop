"""
This code is used to evaluate performance of facebook's HRSL dataset in Kenya
"""
import shapely
from shapely.geometry import *
import pandas as pd
import geopandas as gpd
import numpy as np
import code
import pickle
import georasters as gr
from rtree import index

def buildpop(hrsl, builds):
    print("adding pop column")
    def assign_pop(dg):
        gps_x = dg["gps_x"]
        gps_y = dg["gps_y"]
        try:
            return hrsl.map_pixel(gps_x, gps_y)
        except:
            return "out_of_bounds"
    # builds["pop"] = builds.apply(lambda x: hrsl.map_pixel(x["gps_x"], x["gps_y"]), axis=1)
    builds["pop"] = builds.apply(assign_pop, axis=1)
    return builds

def updated_landcover(bd, lands):
    # Assigning landcover using georasters
    print("Adding updated landcover")
    def assign_landcover(dg):
        gps_x = dg["gps_x"]
        gps_y = dg["gps_y"]
        try:
            return lands.map_pixel(gps_x, gps_y)
        except:
            print("Unknown land")
            return "unknown"
    bd["new_landcover"] = bd.apply(assign_landcover, axis=1)
    print("Updated landcovers assigned")
    # code.interact(local = locals())
    return bd

if __name__=='__main__':
    hrsl_path = "/Users/zealshah/Documents/deep_learning_project/hrsl_ken_v2/population_ken_2018-10-01.tif"
    survey_path = "/Users/zealshah/Documents/deep_learning_project/kenya_struct_locs/structLocs_landcover.pck"
    land_path = "/Users/zealshah/Documents/deep_learning_project/land_use_data_processing/land_use_data/geokenya.tif"

    hrsl = gr.from_file(hrsl_path)
    builds = pd.read_pickle(survey_path)
    lands = gr.from_file(land_path)
    print("data read")
    db = buildpop(hrsl, builds)
    print("population done")
    dl = updated_landcover(db, lands)
    print("landcover done")
    dl.to_pickle("buildpop.pck")

    code.interact(local = locals())
