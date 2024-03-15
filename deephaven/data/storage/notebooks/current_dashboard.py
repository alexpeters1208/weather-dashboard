import deephaven.plot.express as dx
import deephaven.ui as ui

import deephaven.time as dhtu
import deephaven.pandas as dhpd
from deephaven.table_listener import listen
from deephaven import merge


### TOP LEVEL PANELS

@ui.component
def select_dashboard_topic():
    return ui.panel(
        ui.button_group(
            ui.button("Current Weather"),
            ui.button("Historcial Weather"),
            ui.button("Weather Forecasts")),
        title="Selection Panel"
    )

@ui.component
def get_last_updated(table):
    data = ui.use_table_data(table.select(["last_updated", "tz_id"]))
    last_updated = data["last_updated"][0].tz_convert(data["tz_id"][0])
    
    return ui.panel(
        ui.text("Last updated: ", str(last_updated)),
        title="Last Updated"
    )

@ui.component
def get_current_location(table):
    data = ui.use_table_data(table.select(["name", "region", "country", "condition"]))
    
    return ui.panel(
        ui.text(str(data["name"][0]) + ", " + str(data["region"][0]) + ", " + str(data["country"][0])),
        ui.text(str(data["condition"][0])),
        title="Current Location"
    )


### ENTIRE STATE HEATMAPS

def current_texas_map(table, response, title_response, **kwargs):
    fig = dx.scatter_mapbox(
        table=table.update("size=45"),
        lat="lat",
        lon="lon",
        color=response,
        title=f"Current {title_response} across Texas",
        hover_name="name",
        size="size",
        opacity=0.075,
        zoom=5,
        center={'lat': 31.21, 'lon': -100.14},
        mapbox_style='carto-positron',
        **kwargs
    )
    return fig

@ui.component
def get_current_temp_map(table):
    return ui.panel(current_texas_map(
        table=table,
        response="temp_f",
        title_response="temperature",
        color_continuous_scale="plasma"),
        title="Temperature"
    )

@ui.component
def get_current_feels_like_map(table):
    return ui.panel(current_texas_map(
        table=table,
        response="feelslike_f",
        title_response="perceived temperature",
        color_continuous_scale="viridis"),
        title="Feels like"
    )

@ui.component
def get_current_wind_speed_map(table):
    return ui.panel(current_texas_map(
        table=table,
        response="wind_mph",
        title_response="wind speed",
        color_continuous_scale="oranges"),
        title="Wind Speed"
    )

@ui.component
def get_current_humidity_map(table):
    return ui.panel(current_texas_map(
        table=table,
        response="humidity",
        title_response="humidity",
        range_color=[0, 100],
        color_continuous_scale="bluyl"),
        title="Humidity"
    )

@ui.component
def get_current_cloud_coverage_map(table):
    return ui.panel(current_texas_map(
        table=table,
        response="cloud",
        title_response="cloud coverage",
        range_color=[0, 100],
        color_continuous_scale="pubu"),
        title="Cloud Coverage"
    )


### CURRENT LOCATION DATA

@ui.component
def get_current_statistics(table):
    data = ui.use_table_data(table)

    return ui.panel(
        ui.text("Current temperature: ", str(data["temp_f"][0]), " degrees"),
        ui.text("Feels like: ", str(data["feelslike_f"][0]), " degrees"),
        ui.text("Wind speed: ", str(data["gust_mph"][0]), " mph"),
        ui.text("Humidity: ", str(data["humidity"][0]), "%"),
        ui.text("Cloud coverage: ", str(data["cloud"][0]), "%"),
        title="Statistics"
    )

@ui.component
def get_current_statistics_chart(current_table, historical_table):
    column_key = {
        "Temperature": ("temp_f", "#DF5BB9"), "Feels like": ("feelslike_f", "#DF5BB9"),
        "Wind speed": ("wind_mph", "#E5732D"), "Humidity": ("humidity", "#0FDDB1"),
        "Cloud coverage": ("cloud", "#0FBFDD")}
    
    response, set_response = ui.use_state("Temperature")

    picker = ui.picker(
        *list(column_key.keys()),
        on_selection_change=set_response,
        selected_key=response,
    )

    current_data = ui.use_table_data(current_table)
    latest = current_data['last_updated'][0]

    def query():
        j_latest = dhtu.to_j_instant(latest)
        return merge([
            current_table \
                .select(["last_updated", "temp_f", "wind_mph", "humidity", "cloud", "feelslike_f", "tz_id"]) \
                .rename_columns("time=last_updated"),
            historical_table \
                .where("toLocalTime(time, tz_id) == toLocalTime(lowerBin(j_latest, HOUR), tz_id)") \
                .tail(6) \
                .select(["time", "temp_f", "wind_mph", "humidity", "cloud", "feelslike_f", "tz_id"]) \
                .reverse()
        ]).update("day = dayOfWeek(time, tz_id)").reverse()

    recent_stats = ui.use_memo(query, [current_table, historical_table])

    return ui.panel(
        picker,
        dx.bar(
            table=recent_stats,
            x="day",
            y=column_key[response][0],
            title=f"{response} at this time for past 7 days"
        ),
        title="Statistics"
    )

@ui.component
def get_current_air_quality(table):

    data = ui.use_table_data(table)

    return ui.panel(
        ui.text("Carbon monoxide: ", str(data["co"][0]), " mcg/m3"),
        ui.text("Ozone: ", str(data["o3"][0]), " mcg/m3"),
        ui.text("Nitrogen dioxide: ", str(data["no2"][0]), " mcg/m3"),
        ui.text("Sulphur dioxide: ", str(data["no2"][0]), " mcg/m3"),
        ui.text("PM2.5: ", str(data["pm2_5"][0]), " mcg/m3"),
        ui.text("PM10: ", str(data["pm10"][0]), " mcg/m3"),
        title="Air Quality Statistics"
    )

@ui.component
def get_current_air_quality_overall(table):

    overall = ui.use_cell_data(table.select("us_epa_index"))

    quality_key = {1: ("Good", "#B1FFAA"), 2: ("Moderate", "#E3FFAA"), 3: ("Unhealthy for some", "#FFF2AA"),
        4: ("Unhealthy for most", "#FFD1AA"), 5: ("Very unhealthy", "#FFAAAA"), 6: ("Hazardous", "#FF7F7F")}

    return ui.panel(
        ui.html.div(
            ui.text("Overall air quality assessment by US-EPA standard: "),
            ui.text(quality_key[overall][0]),
            title="Overall Air Quality",
            style={
                "width": "100%",
                "height": "100%",
                "background-color": quality_key[overall][1]
            }
        ),
        title="Overall Air Quality"
    )


### DASHBOARD

current_dashboard = ui.dashboard(
    ui.column(

        ui.row(
            ui.column(
                select_dashboard_topic(),
                width=30
            ),
            ui.column(
                get_last_updated(current_here),
                width=25
            ),
            ui.column(
                get_current_location(current_here),
                width=22.5
            ),
            ui.column(
                get_current_air_quality_overall(current_here),
                width=22.5
            ),
            height=20
        ),

        ui.row(
            ui.column(
                get_current_temp_map(current),
                get_current_wind_speed_map(current),
                get_current_humidity_map(current),
                get_current_cloud_coverage_map(current),
                width=55
            ),
            ui.column(
                ui.row(
                    ui.column(
                        get_current_statistics(current_here),
                        width=22.5
                    ),
                    ui.column(
                        get_current_air_quality(current_here),
                        width=22.5
                    ),
                    height=25
                ),
                ui.row(
                    get_current_statistics_chart(current_here, historical_here),
                    height=75
                ),
                width=45
            ),
            height=80
        )
    )
)
