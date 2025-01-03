import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv()
API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")


def search_google_books(query):
    """Fetch all books for a given query using the Google Books API."""
    url = "https://www.googleapis.com/books/v1/volumes"
    books = []
    start_index = 0  # Pagination start index

    while True:
        params = {
            "q": query,  # Search query
            "startIndex": start_index,
            "maxResults": 40,  # Fetch up to 40 books at a time
            "langRestrict": "pt",  # Restrict results to Portuguese
            "printType": "books",  # Only fetch books
            "key": API_KEY,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Process books from the response
        for item in data.get("items", []):
            volume_info = item.get("volumeInfo", {})
            sale_info = item.get("saleInfo", {})

            books.append(
                {
                    "title": volume_info.get("title", "N/A"),
                    "authors": ", ".join(volume_info.get("authors", [])),
                    "averageRating": volume_info.get("averageRating", "N/A"),
                    "ratingsCount": volume_info.get("ratingsCount", "N/A"),
                    "publishedDate": volume_info.get("publishedDate", "N/A"),
                    "description": volume_info.get("description", "N/A"),
                    "price": sale_info.get("listPrice", {}).get("amount", "N/A"),
                    "currency": sale_info.get("listPrice", {}).get(
                        "currencyCode", "N/A"
                    ),
                }
            )

        # Check if there are more results
        if "items" not in data or len(data.get("items", [])) < 40:
            break

        # Increment the start index for the next batch
        start_index += 40

    return books


def save_books_to_json(books, filename="books.json"):
    """Save book data to a CSV file."""
    df = pd.DataFrame(books)
    df.to_json(filename, index=False)
    print(f"Data saved to {filename}")


# Main execution
if __name__ == "__main__":
    query = "bestsellers"
    print(f"Fetching all books for query: '{query}'...")

    # Fetch all books
    books = search_google_books(query)

    # Save results to a CSV file
    save_books_to_json(books, filename="google_books_bestsellers.json")
    print("Done!")
