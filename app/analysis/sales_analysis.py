from snowflake.snowpark.functions import col
import altair as alt

# 1. KPI: Revenue by Author


def kpi_revenue_by_author(session):
    df = (
        session.table("REVENUE_BY_AUTHOR")
        .select(col("AUTHOR_NAME"), col("TOTAL_REVENUE"))
        .sort(col("TOTAL_REVENUE").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("TOTAL_REVENUE", title="Total Revenue"),
            y=alt.Y("AUTHOR_NAME", sort="-x", title="Author"),
            color="TOTAL_REVENUE",
        )
        .properties(title="Top 10 Authors by Revenue")
    )
    return chart


# 2. KPI: Revenue by Publisher


def kpi_revenue_by_publisher(session):
    df = (
        session.table("REVENUE_BY_PUBLISHER")
        .select(col("PUBLISHER_NAME"), col("TOTAL_REVENUE"))
        .sort(col("TOTAL_REVENUE").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("TOTAL_REVENUE", title="Total Revenue"),
            y=alt.Y("PUBLISHER_NAME", sort="-x", title="Publisher"),
            color="TOTAL_REVENUE",
        )
        .properties(title="Top 10 Publishers by Revenue")
    )
    return chart


# 3. KPI: Sales by Genre


def kpi_sales_by_genre(session):
    df = (
        session.table("SALES_BY_GENRE")
        .select(col("GENRE"), col("TOTAL_REVENUE"))
        .sort(col("TOTAL_REVENUE").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("TOTAL_REVENUE", title="Total Revenue"),
            y=alt.Y("GENRE", sort="-x", title="Genre"),
            color="TOTAL_REVENUE",
        )
        .properties(title="Top 10 Genres by Revenue")
    )
    return chart


# 4. KPI: Top Selling Authors


def kpi_top_selling_authors(session):
    # Fetch the data from the session table
    df = (
        session.table("TOP_SELLING_AUTHORS")
        .select(col("AUTHOR_NAME"), col("TOTAL_BOOKS_SOLD"))
        .to_pandas()
    )

    # Group by author and sum the total books sold
    grouped_df = (
        df.groupby("AUTHOR_NAME", as_index=False)["TOTAL_BOOKS_SOLD"]
        .sum()
        .sort_values("TOTAL_BOOKS_SOLD", ascending=False)
        .head(10)  # Get the top 10 authors
    )

    # Create the bar chart using Altair
    chart = (
        alt.Chart(grouped_df)
        .mark_bar()
        .encode(
            x=alt.X("TOTAL_BOOKS_SOLD", title="Total Books Sold"),
            y=alt.Y("AUTHOR_NAME", sort="-x", title="Author"),
            color="TOTAL_BOOKS_SOLD:Q",
        )
        .properties(title="Top 10 Authors by Books Sold")
    )
    return chart


# 5. KPI: Top Selling Books by Publisher


def kpi_top_selling_books_by_publisher(session):
    df = (
        session.table("TOP_SELLING_BOOKS_BY_PUBLISHER")
        .select(col("TITLE"), col("TOTAL_REVENUE"))
        .sort(col("TOTAL_REVENUE").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("TOTAL_REVENUE", title="Total Revenue"),
            y=alt.Y("TITLE", sort="-x", title="Book Title"),
            color="TOTAL_REVENUE",
        )
        .properties(title="Top 10 Books by Revenue (Publisher)")
    )
    return chart


# 6. KPI: Top Selling Books by Year


def kpi_top_selling_books_by_year(session):
    df = (
        session.table("TOP_SELLING_BOOKS_BY_YEAR")
        .select(col("TITLE"), col("TOTAL_REVENUE"), col("SALES_YEAR"))
        .sort(col("SALES_YEAR").desc(), col("TOTAL_REVENUE").desc())
        .limit(10)
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("TOTAL_REVENUE", title="Total Revenue"),
            y=alt.Y("TITLE", sort="-x", title="Book Title"),
            color="SALES_YEAR:N",
        )
        .properties(title="Top 10 Books by Revenue (Yearly)")
    )
    return chart


# 7. KPI: Yearly Sales Trends


def kpi_yearly_sales_trends(session):
    df = (
        session.table("YEARLY_SALES_TRENDS")
        .select(col("SALES_YEAR"), col("TOTAL_REVENUE"))
        .sort(col("SALES_YEAR"))
        .to_pandas()
    )
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("SALES_YEAR", title="Year"),
            y=alt.Y("TOTAL_REVENUE", title="Total Revenue"),
        )
        .properties(title="Yearly Sales Trends")
    )
    return chart
