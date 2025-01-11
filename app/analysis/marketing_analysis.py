from snowflake.snowpark.functions import col
import altair as alt
import pandas as pd

# 1. KPI: Rating Trends by Year


def kpi_rating_trends_by_year(session):
    # Fetch the data from the session table
    df = (
        session.table("RATING_TRENDS_BY_YEAR")
        .select(col("PUBLICATION_YEAR"), col("AVG_RATING"))
        .to_pandas()
    )

    # Convert PUBLICATION_YEAR to a datetime format and extract only the year
    df["PUBLICATION_YEAR"] = pd.to_datetime(df["PUBLICATION_YEAR"]).dt.year

    # Group by the year and calculate the average rating for each year
    grouped_df = (
        df.groupby("PUBLICATION_YEAR", as_index=False)["AVG_RATING"]
        .mean()
        .sort_values("PUBLICATION_YEAR")
    )

    # Create the line graph using Altair
    chart = (
        alt.Chart(grouped_df)
        .mark_line()
        .encode(
            x=alt.X(
                "PUBLICATION_YEAR:O", title="Year"
            ),  # Ordinal scale for discrete years
            y=alt.Y("AVG_RATING", title="Average Rating"),
        )
        .properties(title="Rating Trends by Year")
    )

    return chart


def kpi_rating_genre_trends_by_year(session):
    # Fetch the data from the session table
    df = (
        session.table("RATING_TRENDS_BY_YEAR")
        .select(col("PUBLICATION_YEAR"), col("AVG_RATING"))
        .to_pandas()
    )

    # Convert PUBLICATION_YEAR to a datetime format and extract only the year
    df["PUBLICATION_YEAR"] = pd.to_datetime(df["PUBLICATION_YEAR"]).dt.year

    # Group by the year and calculate the average rating for each year
    grouped_df = (
        df.groupby("PUBLICATION_YEAR", as_index=False)["AVG_RATING"]
        .mean()
        .sort_values("PUBLICATION_YEAR")
    )

    # Create the line graph using Altair
    chart = (
        alt.Chart(grouped_df)
        .mark_line()
        .encode(
            x=alt.X(
                "PUBLICATION_YEAR:O", title="Year"
            ),  # Ordinal scale for discrete years
            y=alt.Y("AVG_RATING", title="Average Rating"),
        )
        .properties(title="Rating Trends by Year")
    )

    return chart


def kpi_rating_author_trends_by_year(session):
    # Fetch the data from the session table
    df = (
        session.table("RATING_TRENDS_BY_YEAR")
        .select(col("PUBLICATION_YEAR"), col("AVG_RATING"))
        .to_pandas()
    )

    # Convert PUBLICATION_YEAR to a datetime format and extract only the year
    df["PUBLICATION_YEAR"] = pd.to_datetime(df["PUBLICATION_YEAR"]).dt.year

    # Group by the year and calculate the average rating for each year
    grouped_df = (
        df.groupby("PUBLICATION_YEAR", as_index=False)["AVG_RATING"]
        .mean()
        .sort_values("PUBLICATION_YEAR")
    )

    # Create the line graph using Altair
    chart = (
        alt.Chart(grouped_df)
        .mark_line()
        .encode(
            x=alt.X(
                "PUBLICATION_YEAR:O", title="Year"
            ),  # Ordinal scale for discrete years
            y=alt.Y("AVG_RATING", title="Average Rating"),
        )
        .properties(title="Rating Trends by Year")
    )

    return chart


# 2. KPI: Top Rated Books


def kpi_top_rated_books(session):
    df = (
        session.table("TOP_RATED_BOOKS")
        .select(col("TITLE"), col("RATING"))
        .sort(col("RATING").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("RATING", title="Average Rating"),
            y=alt.Y("TITLE", sort="-x", title="Book Title"),
            color="RATING",
        )
        .properties(title="Top 10 Rated Books")
    )
    return chart


# 3. KPI: Top Rated Genres


def kpi_top_rated_genres(session):
    df = (
        session.table("TOP_RATED_GENRES")
        .select(col("GENRE"), col("AVG_RATING"))
        .sort(col("AVG_RATING").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("AVG_RATING", title="Average Rating"),
            y=alt.Y("GENRE", sort="-x", title="Genre"),
            color="AVG_RATING",
        )
        .properties(title="Top 10 Rated Genres")
    )
    return chart
