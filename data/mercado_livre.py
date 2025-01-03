import requests
from bs4 import BeautifulSoup
import pandas as pd
import time


def fetch_page(url):
    """Fetch page content with basic error handling"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching page {url}: {e}")
        return None


def get_next_page_url(soup):
    """Extract the next page URL from pagination"""
    next_button = soup.find("a", {"title": "Seguinte"})
    if next_button:
        return next_button.get("href")
    return None


def parse_category_page(html):
    soup = BeautifulSoup(html, "html.parser")

    # Find all elements for each field
    titles = soup.find_all("h2", class_="poly-box poly-component__title")
    reviews = soup.find_all("span", class_="poly-reviews__rating")
    prices = soup.find_all(
        "span", class_="andes-money-amount andes-money-amount--cents-superscript"
    )

    # Get current timestamp
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    # Create list to store all products
    products = []

    # Use the length of titles to determine how many products we found
    for i in range(len(titles)):
        product = {
            "title": titles[i].text.strip() if i < len(titles) else None,
            "review": reviews[i].text.strip() if i < len(reviews) else None,
            "price": prices[i].text.strip() if i < len(prices) else None,
            "timestamp": timestamp,
        }
        products.append(product)

    return products, soup


def save_to_dataframe(products, df):
    """Salva múltiplos produtos no DataFrame."""
    new_rows = pd.DataFrame(products)
    if df.empty:
        return new_rows
    return pd.concat([df, new_rows], ignore_index=True)


def scrape_all_pages(max_pages=None):
    """Scrape all available pages or up to max_pages if specified"""
    base_url = "https://lista.mercadolivre.com.br/livros-revistas-comics/_Container_os-livros-mais-vendidos_NoIndex_True"
    current_url = base_url
    page_num = 1
    df = pd.DataFrame()

    while current_url:
        print(f"Scraping page {page_num}...")

        # Fetch and parse the page
        page_content = fetch_page(current_url)
        if not page_content:
            break

        products_info, soup = parse_category_page(page_content)

        # Save products to DataFrame
        df = save_to_dataframe(products_info, df)

        # Check if we should continue
        if max_pages and page_num >= max_pages:
            break

        # Get next page URL
        current_url = get_next_page_url(soup)
        page_num += 1

        # Add a small delay to be nice to the server
        time.sleep(2)

    return df


# Teste das funções
if __name__ == "__main__":
    # Scrape all pages (or specify max_pages for testing)
    df = scrape_all_pages(max_pages=5)  # Set to None to scrape all pages

    # Exibe o número total de produtos encontrados
    print(f"\nTotal products found: {len(df)}")

    # Exibe as primeiras linhas do DataFrame
    print("\nFirst few products:")
    print(df.head())

    # Save to CSV
    filename = f'mercadolivre_books_{time.strftime("%Y%m%d_%H%M%S")}.json'
    df.to_json(filename, index=False)
    print(f"\nData saved to {filename}")
