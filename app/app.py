import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, sum as snowflake_sum
import altair as alt

# Page Configuration
st.set_page_config(page_title="DBT Marts Dashboard", layout="wide")


# Connect to Snowflake using Snowpark
@st.cache_resource
def connect_to_snowflake():
    connection_parameters = {
        "account": st.secrets["snowflake_account"],
        "user": st.secrets["snowflake_user"],
        "password": st.secrets["snowflake_password"],
        "role": st.secrets["snowflake_role"],
        "warehouse": st.secrets["snowflake_warehouse"],
        "database": st.secrets["snowflake_database"],
        "schema": st.secrets["snowflake_schema"],
    }
    return Session.builder.configs(connection_parameters).create()


# Query Function using Snowpark
@st.cache_data
def run_query(query):
    session = connect_to_snowflake()
    df = session.sql(query).to_pandas()
    return df


# Rolling Bar (Selectbox) for Navigation
st.title("🗊 DBT Marts Analysis Dashboard")
selected_tab = st.selectbox("Select a Department", ["Dimensions", "Marketing", "Sales"])

# Dimensions Tab
if selected_tab == "Dimensions":
    st.title("📓 Dimensions Analysis")
    st.markdown("Explore foundational data like authors, books, and genres.")

    # Dimensions: Authors
    st.subheader("Authors Dimension")
    session = connect_to_snowflake()
    try:
        authors_df = session.table("dim_authors").limit(100).to_pandas()
        if not authors_df.empty:
            st.write(authors_df)
            st.bar_chart(authors_df["author_name"].value_counts())
        else:
            st.warning("No data available for Authors Dimension.")
    except Exception as e:
        st.error(f"Failed to load Authors Dimension: {e}")

    # Dimensions: Books
    st.subheader("Books Dimension")
    try:
        books_df = session.table("dim_books").limit(100).to_pandas()
        if not books_df.empty:
            st.write(books_df)
            st.bar_chart(books_df["book_title"].value_counts())
        else:
            st.warning("No data available for Books Dimension.")
    except Exception as e:
        st.error(f"Failed to load Books Dimension: {e}")

    # Dimensions: Genres
    st.subheader("Genres Dimension")
    try:
        genres_df = session.table("dim_genres").limit(100).to_pandas()
        if not genres_df.empty:
            st.write(genres_df)
            st.bar_chart(genres_df["genre_name"].value_counts())
        else:
            st.warning("No data available for Genres Dimension.")
    except Exception as e:
        st.error(f"Failed to load Genres Dimension: {e}")

# Marketing Tab
if selected_tab == "Marketing":
    st.title("📈 Marketing Analysis")
    st.markdown("Analyze trends, ratings, and top-performing books or genres.")

    # Marketing: Rating Trends by Year
    st.subheader("Rating Trends by Year")
    try:
        rating_trends_df = session.table("rating_trends_by_year").to_pandas()
        if not rating_trends_df.empty:
            st.line_chart(rating_trends_df.set_index("year")["average_rating"])
        else:
            st.warning("No data available for Rating Trends by Year.")
    except Exception as e:
        st.error(f"Failed to load Rating Trends: {e}")

    # Marketing: Top Rated Books
    st.subheader("Top Rated Books")
    try:
        top_rated_books_df = session.table("top_rated_books").limit(10).to_pandas()
        st.table(top_rated_books_df)
    except Exception as e:
        st.error(f"Failed to load Top Rated Books: {e}")

    # Marketing: Top Rated Genres
    st.subheader("Top Rated Genres")
    try:
        top_rated_genres_df = session.table("top_rated_genres").to_pandas()
        if not top_rated_genres_df.empty:
            st.bar_chart(top_rated_genres_df.set_index("genre")["average_rating"])
        else:
            st.warning("No data available for Top Rated Genres.")
    except Exception as e:
        st.error(f"Failed to load Top Rated Genres: {e}")

# Sales Tab
if selected_tab == "Sales":
    st.title("💰 Sales Metrics")
    st.markdown("Explore revenue, top-selling books, and sales trends.")

    # Sales: Revenue by Author
    st.subheader("Revenue by Author")
    try:
        revenue_author_df = (
            session.table("revenue_by_author")
            .group_by(col("author_name"))
            .agg(snowflake_sum("revenue").alias("total_revenue"))
            .sort(col("total_revenue").desc())
            .limit(10)
            .to_pandas()
        )
        if not revenue_author_df.empty:
            st.bar_chart(revenue_author_df.set_index("author_name")["total_revenue"])
        else:
            st.warning("No data available for Revenue by Author.")
    except Exception as e:
        st.error(f"Failed to load Revenue by Author: {e}")

    # Sales: Yearly Sales Trends
    st.subheader("Yearly Sales Trends")
    try:
        yearly_sales_df = (
            session.table("yearly_sales_trends")
            .group_by(col("year"))
            .agg(snowflake_sum("sales").alias("total_sales"))
            .sort(col("year"))
            .to_pandas()
        )
        if not yearly_sales_df.empty:
            chart = (
                alt.Chart(yearly_sales_df)
                .mark_line()
                .encode(x="year:O", y="total_sales:Q")
                .properties(title="Yearly Sales Trends")
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("No data available for Yearly Sales Trends.")
    except Exception as e:
        st.error(f"Failed to load Yearly Sales Trends: {e}")

    # Sales: Top Selling Books by Publisher
    st.subheader("Top Selling Books by Publisher")
    try:
        top_books_df = (
            session.table("top_selling_books_by_publisher")
            .group_by(col("publisher_name"))
            .agg(snowflake_sum("sales").alias("total_sales"))
            .sort(col("total_sales").desc())
            .limit(10)
            .to_pandas()
        )
        st.table(top_books_df)
    except Exception as e:
        st.error(f"Failed to load Top Selling Books by Publisher: {e}")

# End of Application
