"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format


class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream",
                broker="kafka://localhost:9092", store="memory://")
stations_topic = app.topic("cta.db.stations", value_type=Station)
transformed_stations_topic = app.topic(
    "cta.db.stations_transformed", partitions=1, value_type=TransformedStation)

# TODO: Why should I use a table here?
# table = app.Table(
#     "transformed_stations",
#     # default=TODO,
#     partitions=1,
#     changelog_topic=transformed_stations_topic,
# )


@app.agent(stations_topic)
async def map_stations(stations):
    def map_colors_to_line(red: bool, blue: bool, green: bool):
        if (red):
            return 'red'
        if (blue):
            return 'blue'
        if (green):
            return 'green'

    async for station in stations:
        await transformed_stations_topic.send(value=TransformedStation(
            station_id=station.station_id, station_name=station.station_name,
            order=station.order, line=map_colors_to_line(station.red, station.blue, station.green))
        )


if __name__ == "__main__":
    app.main()
