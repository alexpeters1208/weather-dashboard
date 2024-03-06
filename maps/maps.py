import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

df = pd.read_csv('../data/lat-long.csv')


fig = go.Figure(data=go.Scattergeo(
        lon = df['long'],
        lat = df['lat'],
        mode = 'markers'
        ))

fig.update_layout(
        title = 'Most trafficked US airports<br>(Hover for airport names)',
        geo_scope='usa',
    )
fig.show()