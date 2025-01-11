import streamlit as st
from helpers.connection import connect_to_snowflake
from analysis.sales_analysis import (
    kpi_revenue_by_author,
    kpi_yearly_sales_trends,
    kpi_top_selling_books_by_publisher,
    kpi_revenue_by_publisher,
    kpi_sales_by_genre,
    kpi_top_selling_authors,
    kpi_top_selling_books_by_year,
)
from analysis.marketing_analysis import (
    kpi_rating_trends_by_year,
    kpi_top_rated_books,
    kpi_top_rated_genres,
)

# Page Configuration
st.set_page_config(page_title="DBT Marts Dashboard", layout="wide")


# Main App
st.title("📊 DBT Marts Analysis Dashboard")
st.markdown("Select a department to explore key performance indicators.")

# Sidebar for Department Selection
selected_department = st.selectbox("Select a Department", ["Sales", "Marketing"])

# Connect to Snowflake session
session = connect_to_snowflake()

# Sales Analysis
if selected_department == "Sales":
    st.header("💰 Sales Metrics")

    # Revenue by Author
    st.subheader("Revenue by Author")
    revenue_chart = kpi_revenue_by_author(session)
    st.altair_chart(revenue_chart, use_container_width=True)

    # Yearly Sales Trends
    st.subheader("Yearly Sales Trends")
    sales_trends_chart = kpi_yearly_sales_trends(session)
    st.altair_chart(sales_trends_chart, use_container_width=True)

    # Top Selling Books by Publisher
    st.subheader("Top Selling Books by Publisher")
    top_books_chart = kpi_top_selling_books_by_publisher(session)
    st.altair_chart(top_books_chart, use_container_width=True)

    # Revenue by Publisher
    st.subheader("Revenue by Publisher")
    revenue_publisher_chart = kpi_revenue_by_publisher(session)
    st.altair_chart(revenue_publisher_chart, use_container_width=True)

    # Sales by Genre
    st.subheader("Sales by Genre")
    sales_genre_chart = kpi_sales_by_genre(session)
    st.altair_chart(sales_genre_chart, use_container_width=True)

    # Top Selling Authors
    st.subheader("Top Selling Authors")
    top_authors_chart = kpi_top_selling_authors(session)
    st.altair_chart(top_authors_chart, use_container_width=True)

    # Top Selling Books by Year
    st.subheader("Top Selling Books by Year")
    top_books_year_chart = kpi_top_selling_books_by_year(session)
    st.altair_chart(top_books_year_chart, use_container_width=True)

# Marketing Analysis
elif selected_department == "Marketing":
    st.header("📈 Marketing Analysis")

    # Rating Trends by Year
    st.subheader("Rating Trends by Year")
    rating_trends_chart = kpi_rating_trends_by_year(session)
    st.altair_chart(rating_trends_chart, use_container_width=True)

    # Top Rated Books
    st.subheader("Top Rated Books")
    top_rated_books_chart = kpi_top_rated_books(session)
    st.altair_chart(top_rated_books_chart, use_container_width=True)

    # Top Rated Genres
    st.subheader("Top Rated Genres")
    top_rated_genres_chart = kpi_top_rated_genres(session)
    st.altair_chart(top_rated_genres_chart, use_container_width=True)
