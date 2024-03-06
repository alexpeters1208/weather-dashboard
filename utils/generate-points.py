import geopandas as gpd
import pandas as pd
import numpy as np

# Load the shapefile of Texas border and convert to lat-long
texas_shapefile = gpd.read_file("texas-shapefile/State.shp")  # Replace "path_to_shapefile" with the actual path
texas_shapefile_lat_long = texas_shapefile.to_crs(epsg=4326)

# Get bounding box of Texas
min_long, min_lat, max_long, max_lat = texas_shapefile_lat_long.geometry.total_bounds

# Define grid parameters
grid_spacing = 0.5  # Adjust this value to change the grid spacing

# Generate grid points within the bounding box
grid_points = []
long_coords = np.arange(min_long, max_long, grid_spacing)
lat_coords = np.arange(min_lat, max_lat, grid_spacing)

# Add coordinates within Texas border to grid_points
for lat in lat_coords:
    for long in long_coords:
        point = (round(lat, 4), round(long, 4))
        if texas_shapefile_lat_long.contains(
                gpd.geoseries.GeoSeries(gpd.points_from_xy([point[1]], [point[0]], crs="EPSG:4326"))).any():
            grid_points.append(point)

# Save grid_points to csv
grid_points_df = pd.DataFrame(grid_points, columns=["lat", "long"]).sort_values(["lat", "long"])
grid_points_df.to_csv("../data/lat-long.csv", index=False)