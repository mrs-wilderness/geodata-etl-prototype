import pandas as pd
from sqlalchemy import create_engine, Table, MetaData

from src.etl import helpers, config

engine = create_engine(f'postgresql+psycopg2://{config.DB_USER}:{config.DB_PASS}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}')
metadata = MetaData()

df_staging = pd.read_csv(config.CSV_PATH)

# populate events table
df_staging[['date_from', 'date_to']] = df_staging[['date_from', 'date_to']].apply(lambda col: col.str.replace('.', ''))
df_staging[['date_from', 'date_to']] = df_staging[['date_from', 'date_to']].apply(lambda col: col.astype('Int64'))
df_staging['date_is_precise'] = df_staging['date_is_precise'].astype(bool)
cols_to_strip = ['date_description', 'event_name', 'event_description', 'contributor']
df_staging[cols_to_strip] = df_staging[cols_to_strip].apply(lambda col: col.str.strip())

df_events = df_staging.loc[:, ['record_id', 'event_name', 'date_from', 'date_to', 'date_description', 'date_is_precise', 'event_description', 'contributor']]
df_events.columns = ['external_record_id', 'event_name', 'start_date_int', 'end_date_int', 'date_description', 'precise_date', 'event_description', 'contributor_name']
events_list = df_events.to_dict(orient='records')

events_table = Table('events', metadata, autoload_with=engine)
with engine.begin() as conn:
    result = helpers.insert_with_returning(
        conn,
        events_table,
        events_list,
        [events_table.c.event_id, events_table.c.external_record_id]
    )

df_staging = helpers.merge_generated_ids(
    df_staging,
    result,
    left_on='record_id',
    returned_cols=['event_id', 'external_record_id'],
    drop_cols=['external_record_id']
)

# populate locations table
df_loc_coord = df_staging.loc[:, ['event_id', 'location_text', 'location_type', 'coordinates_lat_lon', 'location_is_exact']]
cols_to_explode = ['location_text', 'location_type', 'location_is_exact', 'coordinates_lat_lon']
df_loc_coord = helpers.explode_and_strip(df_loc_coord, cols_to_explode)

df_locations = df_loc_coord.loc[:, ['location_text', 'location_type']]
df_locations = df_locations.drop_duplicates()
df_locations.columns = ['location_description', 'geometry_type']
locations_list = df_locations.to_dict(orient='records')

locations_table = Table('locations', metadata, autoload_with=engine)
with engine.begin() as conn:
    result = helpers.insert_with_returning(
        conn,
        locations_table,
        locations_list,
        [locations_table.c.location_id, locations_table.c.location_description]
    )

df_loc_coord = helpers.merge_generated_ids(
    df_loc_coord,
    result,
    left_on='location_text',
    returned_cols=['location_id', 'location_description'],
    drop_cols=['location_description']
)


# populate event_location table
df_event_location = df_loc_coord.loc[:, ['event_id', 'location_id', 'location_is_exact']].drop_duplicates()
df_event_location['location_is_exact'] = df_event_location['location_is_exact'].astype(bool)
df_event_location.rename(columns={'location_is_exact': 'precise_location'}, inplace=True)
event_location_list = df_event_location.to_dict(orient='records')

event_location_table = Table('event_location', metadata, autoload_with=engine)
with engine.begin() as conn:
    helpers.insert_simple(conn, event_location_table, event_location_list)

# populate coordinates table
df_loc_coord.drop(columns=['event_id', 'location_text', 'location_type', 'location_is_exact'], inplace=True)
df_loc_coord['coordinates_lat_lon'] = df_loc_coord['coordinates_lat_lon'].str.split('-')
df_loc_coord['point_number'] = df_loc_coord['coordinates_lat_lon'].apply(len).apply(range).apply(list)
df_loc_coord = df_loc_coord.explode(['coordinates_lat_lon', 'point_number'])
df_loc_coord[['latitude', 'longitude']] = df_loc_coord['coordinates_lat_lon'].str.split(',', expand=True)
df_loc_coord[['latitude', 'longitude']] = df_loc_coord[['latitude', 'longitude']].astype(float)
df_loc_coord.drop(columns=['coordinates_lat_lon'], inplace=True)

df_coordinates = df_loc_coord.loc[:, ['longitude', 'latitude']].drop_duplicates()
coordinates_list = df_coordinates.to_dict(orient='records')

coordinates_table = Table('coordinates', metadata, autoload_with=engine)
with engine.begin() as conn:
    result = helpers.insert_with_returning(
        conn,
        coordinates_table,
        coordinates_list,
        [
            coordinates_table.c.coordinates_id,
            coordinates_table.c.longitude,
            coordinates_table.c.latitude
        ]
    )

# special case: coordinates require multi-key merge; helper not applicable
db_generated_ids = pd.DataFrame(result.fetchall(), columns=['coordinates_id', 'longitude', 'latitude'])
db_generated_ids[['latitude', 'longitude']] = db_generated_ids[['latitude', 'longitude']].astype(float)
df_loc_coord = df_loc_coord.merge(db_generated_ids, on=['latitude', 'longitude'], how='left')
df_loc_coord.drop(columns=['latitude', 'longitude'], inplace=True)

# populate location_coordinates table
df_loc_coord = df_loc_coord.drop_duplicates()
location_coordinates_list = df_loc_coord.to_dict(orient='records')

location_coordinates_table = Table('location_coordinates', metadata, autoload_with=engine)
with engine.begin() as conn:
    helpers.insert_simple(conn, location_coordinates_table, location_coordinates_list)

# populate tags table
df_event_tag = df_staging.loc[:, ['event_id', 'tags']]
df_event_tag = helpers.explode_and_strip(df_event_tag, ['tags'])

df_tags = df_event_tag.loc[:, ['tags']].drop_duplicates()
df_tags.rename(columns={'tags': 'tag_name'}, inplace=True)
tags_list = df_tags.to_dict(orient='records')

tags_table = Table('tags', metadata, autoload_with=engine)
with engine.begin() as conn:
    result = helpers.insert_with_returning(
        conn,
        tags_table,
        tags_list,
        [tags_table.c.tag_id, tags_table.c.tag_name]
    )

df_event_tag = helpers.merge_generated_ids(
    df_event_tag,
    result,
    left_on='tags',
    returned_cols=['tag_id', 'tag_name'],
    drop_cols=['tags', 'tag_name']
)

# populate event_tag table
df_event_tag = df_event_tag.drop_duplicates()
event_tag_list = df_event_tag.to_dict(orient='records')

event_tag_table = Table('event_tag', metadata, autoload_with=engine)
with engine.begin() as conn:
    helpers.insert_simple(conn, event_tag_table, event_tag_list)

# populate sources table
df_event_source = df_staging.loc[:, ['event_id', 'sources_list']]
df_event_source = helpers.explode_and_strip(df_event_source, ['sources_list'])

df_sources = df_event_source.loc[:, ['sources_list']].drop_duplicates()
df_sources.rename(columns={'sources_list': 'source_value'}, inplace=True)
df_sources['source_type'] = 'url'
sources_list = df_sources.to_dict(orient='records')

sources_table = Table('sources', metadata, autoload_with=engine)
with engine.begin() as conn:
    result = helpers.insert_with_returning(
        conn,
        sources_table,
        sources_list,
        [sources_table.c.source_id, sources_table.c.source_value]
    )

df_event_source = helpers.merge_generated_ids(
    df_event_source,
    result,
    left_on='sources_list',
    returned_cols=['source_id', 'source_value'],
    drop_cols=['source_value', 'sources_list']
)

# populate event_source table
df_event_source = df_event_source.drop_duplicates()
event_source_list = df_event_source.to_dict(orient='records')

event_source_table = Table('event_source', metadata, autoload_with=engine)
with engine.begin() as conn:
    helpers.insert_simple(conn, event_source_table, event_source_list)



