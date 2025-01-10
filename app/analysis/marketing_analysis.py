from snowflake.snowpark.functions import col
import altair as alt

# 1. KPI: Rating Trends by Year


def kpi_rating_trends_by_year(session):
    df = (
        session.table("RATING_TRENDS_BY_YEAR")
        .select(col("YEAR"), col("AVERAGE_RATING"))
        .sort(col("YEAR"))
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("YEAR", title="Year"),
            y=alt.Y("AVERAGE_RATING", title="Average Rating"),
        )
        .properties(title="Rating Trends by Year")
    )
    return chart


# 2. KPI: Top Rated Books


def kpi_top_rated_books(session):
    df = (
        session.table("TOP_RATED_BOOKS")
        .select(col("TITLE"), col("AVERAGE_RATING"))
        .sort(col("AVERAGE_RATING").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("AVERAGE_RATING", title="Average Rating"),
            y=alt.Y("TITLE", sort="-x", title="Book Title"),
            color="AVERAGE_RATING",
        )
        .properties(title="Top 10 Rated Books")
    )
    return chart


# 3. KPI: Top Rated Genres


def kpi_top_rated_genres(session):
    df = (
        session.table("TOP_RATED_GENRES")
        .select(col("GENRE_NAME"), col("AVERAGE_RATING"))
        .sort(col("AVERAGE_RATING").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("AVERAGE_RATING", title="Average Rating"),
            y=alt.Y("GENRE_NAME", sort="-x", title="Genre"),
            color="AVERAGE_RATING",
        )
        .properties(title="Top 10 Rated Genres")
    )
    return chart
