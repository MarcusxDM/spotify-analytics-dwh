import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('db_connection.cfg')

ARTIST_DATA = config['SOURCE']['ARTIST_RANK']
CHART_DATA = config['SOURCE']['CHART']

# DROP TABLES

staging_artist_table_drop = "DROP TABLE IF EXISTS staging_artists"
staging_chart_table_drop = "DROP TABLE IF EXISTS staging_charts"
staging_countries_table_drop = "DROP TABLE IF EXISTS staging_countries"

artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
calendar_table_drop = "DROP TABLE IF EXISTS dim_calendar"
chart_table_drop = "DROP TABLE IF EXISTS dim_charts"
songs_table_drop = "DROP TABLE IF EXISTS dim_songs"
streams_table_drop = "DROP TABLE IF EXISTS fact_streams"

# CREATE TABLES

staging_artist_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_artists (
        mbid                TEXT,
        artist_mb           TEXT,
        artist_lastfm       TEXT,
        country_mb          VARCHAR,
        country_lastfm      VARCHAR,
        tags_mb             TEXT,
        tags_lastfm         TEXT,
        listeners_lastfm    INT,
        scrobbles_lastfm    INT,
        ambiguous_artist    BOOLEAN
    );
""")

staging_country_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_countries (
        country_id          VARCHAR,
        country_name        VARCHAR,
        continent           VARCHAR
    );
""")

staging_chart_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_charts (
        title   VARCHAR,
        rank    INT,
        date    DATE,
        artist  VARCHAR,
        url     TEXT,
        region  VARCHAR,
        chart   VARCHAR,
        trend   VARCHAR,
        streams INT
    );
""")

copy_artists_table_staging = (
    """COPY staging_artists
        FROM {0}
        DELIMITER ','
        CSV 
        HEADER;
""").format(ARTIST_DATA)

copy_charts_table_staging = (
    """COPY staging_charts
        FROM {0}
        DELIMITER ','
        CSV 
        HEADER;
""").format(CHART_DATA)

insert_countries_table_staging = (
    """INSERT INTO staging_countries(country_id, country_name, continent)
        values(%s, %s, %s);
""")

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_artists (
        artist_id   SERIAL PRIMARY KEY,
        name    TEXT,
        country VARCHAR,
        continent VARCHAR,
        tags    TEXT,
        listeners_lastfm INT,
        scrobbles_lastfm INT
    );
    """
)

streams_table_create= ("""
    CREATE TABLE IF NOT EXISTS fact_streams (
        stream_id SERIAL PRIMARY KEY,
        song_id VARCHAR NOT NULL,
        rank    INT,
        chart_date    DATE NOT NULL,
        artist_id  INT NOT NULL,
        chart_id INT NOT NULL,
        trend   VARCHAR,
        streams INT,
        FOREIGN KEY (song_id) REFERENCES dim_songs (song_id),
        FOREIGN KEY (artist_id) REFERENCES dim_artists (artist_id),
        FOREIGN KEY (chart_id) REFERENCES dim_charts (chart_id),
        FOREIGN KEY (chart_date) REFERENCES dim_calendar (chart_date)
    );
    """
)

charts_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_charts (
        chart_id SERIAL PRIMARY KEY,
        name  VARCHAR,
        region   VARCHAR,
        continent   VARCHAR
    );
    """
)

calendar_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_calendar (
        chart_date DATE PRIMARY KEY,
        year INT,
        month INT,
        week INT,
        weekday VARCHAR,
        day INT 
    );
    """
)

songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_songs (
        song_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        artist_name VARCHAR
    );
""")

"""
Must execute before 
"""
artists_table_insert = (
    """
    INSERT INTO dim_artists (
        name,
        country,
        continent,
        tags,
        listeners_lastfm,
        scrobbles_lastfm)
    
    SELECT DISTINCT ON (c.artist) c.artist, a.country_mb, cou.continent, a.tags_mb, a.listeners_lastfm, a.scrobbles_lastfm
            FROM public.staging_charts as c
            LEFT JOIN (SELECT mbid, artist_mb, artist_lastfm, country_mb, country_lastfm, LOWER(tags_mb) as tags_mb, tags_lastfm, listeners_lastfm, scrobbles_lastfm, ambiguous_artist
                        FROM public.staging_artists
                        where country_mb is not null and
                        tags_mb is not null) as a
            ON c.artist = a.artist_mb
            LEFT JOIN staging_countries as cou
            ON a.country_mb = cou.country_name 
            WHERE c.artist IS NOT NULL
            ORDER BY c.artist;
    """
)

songs_table_insert = (
    """
    INSERT INTO dim_songs (
        song_id,
        name,
        artist_name
    )   SELECT DISTINCT SUBSTRING(url, 32), FIRST_VALUE(title) OVER(PARTITION BY url ORDER BY date DESC), 
                        FIRST_VALUE(artist) OVER(PARTITION BY url ORDER BY date DESC)
                from public.staging_charts
    WHERE url IS NOT NULL;
    """
)

calendar_table_insert = (
    """
    INSERT INTO dim_calendar (
        chart_date,
        year,
        month,
        week,
        weekday,
        day
    )   SELECT DISTINCT 
                date,
                EXTRACT(year from date),
                EXTRACT(month from date),
                EXTRACT(week from date),
                EXTRACT(isodow from date),
                EXTRACT(day from date)
        FROM staging_charts;
    """
)

charts_table_insert = (
    """
    INSERT INTO dim_charts (
        name,
        region,
        continent
    )   SELECT DISTINCT c.chart, c.region, cou.continent
	FROM public.staging_charts as c
    LEFT JOIN staging_countries as cou
    ON c.region = cou.country_name
    WHERE chart IS NOT NULL;
    """
)

streams_table_insert = (
    """
    INSERT INTO fact_streams (
        song_id,
        rank,
        chart_date,
        artist_id,
        chart_id,
        trend,
        streams
    )
    SELECT DISTINCT SUBSTRING(stg.url, 32), stg.rank, stg.date, 
                   a.artist_id, c.chart_id, stg.trend, sum(stg.streams)
	FROM public.staging_charts as stg
    LEFT JOIN dim_artists as a
    ON stg.artist = a.name
    LEFT JOIN dim_charts as c
    ON stg.chart = c.name AND
       stg.region = c.region
    WHERE stg.artist IS NOT NULL
    AND stg.chart IS NOT NULL
    GROUP BY SUBSTRING(stg.url, 32), stg.rank, stg.date, 
                   a.artist_id, c.chart_id, stg.trend;
    """
)

artist_quality_check = """SELECT COUNT(artist_id) 
                        FROM dim_artists
                        WHERE artist_id IS NULL ;"""
song_quality_check = """SELECT COUNT(song_id) 
                        FROM dim_songs
                        WHERE song_id IS NULL ;"""
chart_quality_check = """SELECT COUNT(chart_id) 
                        FROM dim_charts
                        WHERE chart_id IS NULL ;"""
calendar_quality_check = """SELECT COUNT(chart_date) 
                        FROM dim_calendar
                        WHERE chart_date IS NULL ;"""
stream_quality_check = """SELECT COUNT(stream_id) 
                        FROM fact_streams
                        WHERE stream_id IS NULL;"""

# QUERY LISTS


create_table_queries = [staging_artist_table_create, staging_chart_table_create, staging_country_table_create,
                        artist_table_create, charts_table_create, songs_table_create, calendar_table_create, streams_table_create]

copy_table_queries = [copy_artists_table_staging, copy_charts_table_staging]

#  staging_artist_table_drop, staging_chart_table_drop, 
drop_table_queries = [streams_table_drop, artist_table_drop, chart_table_drop,
                     songs_table_drop, calendar_table_drop]

#     
insert_table_queries = [calendar_table_insert, charts_table_insert, songs_table_insert, artists_table_insert, streams_table_insert]

data_quality_checks = [{'sql':artist_quality_check  , 'expected_type':'number', 'expected_result':0},
                    {'sql':song_quality_check  , 'expected_type':'number', 'expected_result':0},
                    {'sql':chart_quality_check  , 'expected_type':'number', 'expected_result':0},
                    {'sql':calendar_quality_check  , 'expected_type':'number', 'expected_result':0},
                    {'sql':stream_quality_check  , 'expected_type':'number', 'expected_result':0}]