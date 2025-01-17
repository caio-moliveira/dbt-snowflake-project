import streamlit as st
from helpers.connection import connect_to_snowflake
from analysis.sales_analysis import (
    kpi_revenue_by_author,
    kpi_revenue_by_publisher,
    kpi_sales_by_genre,
    kpi_top_selling_authors,
    kpi_top_selling_publishers,
    kpi_top_selling_books_by_year,
    kpi_yearly_sales_trends,
)

# Page Configuration
st.set_page_config(page_title="📊 Sales Dashboard", layout="wide")

# Main App
st.title("📊 Sales Analysis Dashboard")
st.markdown(
    "Dive into key sales metrics to uncover insights into authors, genres, books, and yearly trends."
)

# Connect to Snowflake session
session = connect_to_snowflake()

# Organized Sales Analysis Section
with st.container():
    st.header("💰 Revenue Metrics")

    # Revenue by Author
    st.subheader("Top 10 Authors by Revenue")
    revenue_author_chart = kpi_revenue_by_author(session)
    st.altair_chart(revenue_author_chart, use_container_width=True)

    # Revenue by Publisher
    st.subheader("Top 10 Publishers by Revenue")
    revenue_publisher_chart = kpi_revenue_by_publisher(session)
    st.altair_chart(revenue_publisher_chart, use_container_width=True)

    # Sales by Genre
    st.subheader("Top 10 Genres by Revenue")
    sales_genre_chart = kpi_sales_by_genre(session)
    st.altair_chart(sales_genre_chart, use_container_width=True)

with st.container():
    st.header("📚 Metrics by Books Sold")

    # Top Selling Authors
    st.subheader("Top 10 Authors by Books Sold")
    top_authors_chart = kpi_top_selling_authors(session)
    st.altair_chart(top_authors_chart, use_container_width=True)

    # Top Selling Books by Publisher
    st.subheader("Top 10 Publishers by Books Sold")
    top_books_publisher_chart = kpi_top_selling_publishers(session)
    st.altair_chart(top_books_publisher_chart, use_container_width=True)


with st.container():
    st.header("📈 Yearly Sales Trends")

    # Top Selling Books by Year
    st.subheader("Top 10 Books by Revenue (Yearly)")
    top_books_year_chart = kpi_top_selling_books_by_year(session)
    st.altair_chart(top_books_year_chart, use_container_width=True)

    # Yearly Sales Trends
    st.subheader("Yearly Total Revenue Trends")
    yearly_trends_chart = kpi_yearly_sales_trends(session)
    st.altair_chart(yearly_trends_chart, use_container_width=True)
