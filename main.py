from functools import reduce

import h5py
import rasterio
from rasterio.transform import from_bounds
from rasterio.shutil import copy as rio_copy
import numpy as np
import os

# 0.01 - 0.5 mm dark blue 0, 0, 254
# 0.5 - 1 mm light blue 50, 101, 254
# 1 - 2 mm green 127, 127, 0
# 2 - 4 mm yellow 254, 203, 0
# 4 - 8 mm orange 254, 152, 0
# 8 - 16 mm red 254, 0, 0
# 16 - 32 mm pink 254, 0, 254
# 32+ mm whiteish 229, 254, 254

def apply_colour_thresholds(data):
    """
    Convert rainfall intensity values into RGBA colour bands.
    Returns a 4-band (RGBA) numpy array.
    """
    rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)

    def colour(mask, r, g, b):
        rgba[mask] = [r, g, b, 255]

    colour((data >= 0.01) & (data < 0.5), 0, 0, 254)  # dark blue
    colour((data >= 0.5) & (data < 1), 50, 101, 254)  # light blue
    colour((data >= 1) & (data < 2), 127, 127, 0)  # green
    colour((data >= 2) & (data < 4), 254, 203, 0)  # yellow
    colour((data >= 4) & (data < 8), 254, 152, 0)  # orange
    colour((data >= 8) & (data < 16), 254, 0, 0)  # red
    colour((data >= 16) & (data < 32), 254, 0, 254)  # pink
    colour((data >= 32), 229, 254, 254)  # whiteish

    # Make zero or negative values transparent
    rgba[data <= 0] = [0, 0, 0, 0]

    return rgba

def process_radar_file(input_path, output_folder, colour=True, output_filename=None):
    """
    Process a radar HDF5 file and save as Cloud Optimized GeoTIFF (COG).

    If colour=True:
        - Creates a visual RGBA COG using thresholds

    If colour=False:
        - Creates a LOSSLESS single-band COG preserving original values
    """

    os.makedirs(output_folder, exist_ok=True)

    with h5py.File(input_path, "r") as f:
        data = f["dataset1/data1/data"][:]
        where = f["where"].attrs

        ll_lon = where["LL_lon"]
        ll_lat = where["LL_lat"]
        ur_lon = where["UR_lon"]
        ur_lat = where["UR_lat"]

        height, width = data.shape

        transform = from_bounds(ll_lon, ll_lat, ur_lon, ur_lat, width, height)

        # Create temporary normal GeoTIFF first
        temp_tif = os.path.join(output_folder, "temp_output.tif")

        if colour:
            rgba_data = apply_colour_thresholds(data)

            with rasterio.open(
                temp_tif,
                "w",
                driver="GTiff",
                height=height,
                width=width,
                count=4,
                dtype=np.uint8,
                crs="EPSG:4326",
                transform=transform
            ) as dst:
                dst.write(rgba_data[:, :, 0], 1)
                dst.write(rgba_data[:, :, 1], 2)
                dst.write(rgba_data[:, :, 2], 3)
                dst.write(rgba_data[:, :, 3], 4)

            final_name = output_filename or "radar_output_coloured.tif"

        else:
            with rasterio.open(
                temp_tif,
                "w",
                driver="GTiff",
                height=height,
                width=width,
                count=1,
                dtype=data.dtype,
                crs="EPSG:4326",
                transform=transform
            ) as dst:
                dst.write(data, 1)

            final_name = output_filename or "radar_output_greyscale.tif"

    # Final output path
    final_path = os.path.join(output_folder, final_name)

    # Write compressed GeoTIFF directly
    if colour:
        with rasterio.open(
            final_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=4,
            dtype=np.uint8,
            crs="EPSG:4326",
            transform=transform,
            compress="lzw",
            BIGTIFF="IF_SAFER"
        ) as dst:
            dst.write(rgba_data[:, :, 0], 1)
            dst.write(rgba_data[:, :, 1], 2)
            dst.write(rgba_data[:, :, 2], 3)
            dst.write(rgba_data[:, :, 3], 4)
    else:
        with rasterio.open(
            final_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype=data.dtype,
            crs="EPSG:4326",
            transform=transform,
            compress="lzw",
            BIGTIFF="IF_SAFER"
        ) as dst:
            dst.write(data, 1)

    # Clean up temp file
    if os.path.exists(temp_tif):
        os.remove(temp_tif)
    return final_path


# Fix PROJ issue
os.environ["PROJ_LIB"] = r"C:\Users\joshuachewings\PycharmProjects\h5translate\.venv\Lib\site-packages\rasterio\proj_data"
