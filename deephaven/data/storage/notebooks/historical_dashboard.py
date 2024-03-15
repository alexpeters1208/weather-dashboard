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


### HISTORICAL LINE PLOTS

@ui.component
def get_historical_trend_plot(p_table):

    locations = dhpd.to_pandas(p_table.keys())['name'].tolist()
    column_key = {"Temperature": "temp_f", "Feels like": "feelslike_f", "Wind speed": "wind_mph",
        "Humidity": "humidity", "Cloud coverage": "cloud"}

    location, set_location = ui.use_state(locations[0])
    statistic, set_statistic = ui.use_state("Temperature")
    window, set_window = ui.use_state(7)

    location_picker = ui.picker(
        *locations,
        placeholder="Select a location...",
        on_selection_change=set_location,
        selected_key=location,
    )
    statistic_picker = ui.picker(
        *list(column_key.keys()),
        placeholder="Select a statistic...",
        on_selection_change=set_statistic,
        selected_key=statistic,
    )
    window_slider = ui.slider(
        label="Previous Days",
        default_value=window,
        min_value=2,
        max_value=p_table.get_constituent(location).size // 24,
        on_change=set_window,
        step=1
    )

    located_p_table = p_table.get_constituent(location)

    return ui.panel(
        ui.flex(
            location_picker,
            statistic_picker,
            window_slider,
            direction="row"
        ),
        dx.line(
            located_p_table.tail(window * 24),
            x="time",
            y=column_key[statistic],
            title=f"{statistic} in {location} over the last {window} days"
        ),
        title="Historical Trend"
    )


### DASHBOARD

historical_dashboard = ui.dashboard(
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
                ui.text("column1"),
                width=25
            ),
            ui.column(
                get_historical_trend_plot(p_historical),
                width=50
            ),
            ui.column(
                ui.text("adfgs"),
                width=25
            ),
            height=85
        )
    )
)