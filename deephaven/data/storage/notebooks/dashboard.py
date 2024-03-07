import json
import pandas as pd
import plotly.express as px

texas_url = "https://raw.githubusercontent.com/glynnbird/usstatesgeojson/master/texas.geojson"

import urllib.request

def read_geojson(url):
    with urllib.request.urlopen(url) as url:
        jdata = json.loads(url.read().decode())
    return jdata

texas_geojson = read_geojson(texas_url)
texas_coords = [*texas_geojson['geometry']['coordinates'][0][0], *texas_geojson['geometry']['coordinates'][1][0]]
texas_data_df = pd.DataFrame(texas_coords, columns=['lon', 'lat'])
texas_data_df['val'] = [1 for i in range(len(texas_coords))]

# Creating the choropleth map
fig = px.choropleth_mapbox(
    data_frame=texas_data_df,
    geojson=texas_geojson,
    locations=texas_data_df.index,
    color='val',
    color_continuous_scale="Viridis",
    mapbox_style="carto-positron",
    zoom=4.4,
    center={"lat": 31.5, "lon": -100}
)

import plotly.express as px

fig = px.choropleth(
    locations=["TX"],
    locationmode="USA-states",
    scope="usa",
    fitbounds="locations",
    color_discrete_map={"TX": 'Green'})
fig.update_layout(geo = dict(showlakes=False), showlegend=False)
fig.show()