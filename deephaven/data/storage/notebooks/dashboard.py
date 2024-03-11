import deephaven.plot.express as dx
import deephaven.ui as ui
from deephaven.ui import use_state

import deephaven.pandas as dhpd

@ui.component
def get_current_location():
    location_df = dhpd.to_pandas(current_here.select(["name", "region", "country"]))
    location = location_df.name + ", " + location_df.region + ", " + location_df.country
    return ui.text(location[0])

@ui.component
def select_dashboard_topic():
    return ui.button_group(
        ui.button("Current Weather"),
        ui.button("Historcial Weather"),
        ui.button("Weather Forecasts"))

@ui.component
def current_texas_map():
    fig = dx.scatter_geo(
        table=current,
        lat="lat",
        lon="lon",
        color="temp_f",
        fitbounds="locations",
        title="Current temperature across Texas"
    )
    return ui.panel(fig)

texas_weather = ui.dashboard(
    ui.column(
        ui.panel(
            ui.row(
                select_dashboard_topic(),
                height=15
            ), title="Selection Pane"
        ),
        ui.row(
            ui.column(
                current_texas_map(),
                width=65,
                title="Current Weather"
            ),
            ui.column(
                get_current_location(),
                width=35,
                title="Current Stats"
            ),
            height=85
        )
    )
)