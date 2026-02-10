import h5py
import rasterio
from rasterio.transform import from_bounds
from rasterio.shutil import copy as rio_copy
import numpy as np
import os


def apply_colour_thresholds(data):
    """
    Convert rainfall intensity values into RGBA colour bands.
    Returns a 4-band (RGBA) numpy array.
    """
    rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)

    def colour(mask, r, g, b):
        rgba[mask] = [r, g, b, 255]

    colour((data >= 0) & (data < 0.5), 173, 216, 230)   # very light - light blue
    colour((data >= 0.5) & (data < 2), 0, 0, 255)       # light rain - blue
    colour((data >= 2) & (data < 4), 0, 255, 0)         # moderate - green
    colour((data >= 4) & (data < 8), 255, 255, 0)       # heavy - yellow
    colour((data >= 8) & (data < 16), 255, 165, 0)      # very heavy - orange
    colour((data >= 16), 255, 0, 0)                     # extreme - red

    # Make zero or negative values transparent
    rgba[data <= 0] = [0, 0, 0, 0]

    return rgba


def convert_to_cog(src_path, cog_path):
    """
    Convert a regular GeoTIFF to Cloud Optimized GeoTIFF (COG)
    """

    rio_copy(
        src_path,
        cog_path,
        driver="COG",
        compress="lzw",
        BIGTIFF="IF_SAFER"
    )


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

    # Final COG output path
    cog_path = os.path.join(output_folder, final_name)

    # Convert the temp TIFF to a COG
    convert_to_cog(temp_tif, cog_path)

    # Clean up temporary file
    if os.path.exists(temp_tif):
        os.remove(temp_tif)

    return cog_path


# Fix PROJ issue
os.environ["PROJ_LIB"] = r"C:\Users\joshuachewings\PycharmProjects\h5translate\.venv\Lib\site-packages\rasterio\proj_data"
