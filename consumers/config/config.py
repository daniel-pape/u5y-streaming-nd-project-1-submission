# These are the topics used by the producers.
# Their names should match the corresponding names
# in producers/config/config.py.

WEATHER_TOPIC_V1="org.chicago.cta.weather.v1"
STATIONS_TOPIC_V1="org.chicago.cta.stations.table.v1"

ARRIVALS_TOPIC_PREFIX="org.chicago.cta.station.arrivals."

TURNSTILE_SUMMARY_TOPIC="TURNSTILE_SUMMARY"
TURNSTILE_TOPIC_V1="com.udacity.streams.turnstile.v1"
POSTGRES_STATIONS_EXPORT_TOPIC_PREFIX="kafka_export_"
POSTGRES_STATIONS_EXPORT_TOPIC_TABLE="stations"
POSTGRES_STATIONS_EXPORT_TOPIC=f"{POSTGRES_STATIONS_EXPORT_TOPIC_PREFIX}{POSTGRES_STATIONS_EXPORT_TOPIC_TABLE}"
TRANSFORMED_STATIONS_TOPIC="transformed.stations.table"
