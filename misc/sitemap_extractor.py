import csv
import gzip
import logging
import xml.etree.ElementTree as ET
from datetime import datetime

import requests


def extract_sitemap_links(sitemap_url, output_file="sitemap_links.csv"):
    """
    Recursively extract links from XML sitemaps including compressed .gz files
    and save them to a CSV file.

    Args:
        sitemap_url (str): URL of the root sitemap
        output_file (str): Output CSV filename
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Keep track of processed URLs to avoid duplicates
    processed_urls = set()

    # Initialize CSV file with headers
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Last Modified", "Source Sitemap", "Extraction Time"])

    def fetch_and_parse_sitemap(url):
        """
        Helper function to fetch and parse sitemap content, handling both
        regular XML and gzipped files.
        """
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Check if the content is gzipped
            if url.endswith(".gz"):
                content = gzip.decompress(response.content)
            else:
                content = response.content

            return ET.fromstring(content)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None
        except ET.ParseError as e:
            logger.error(f"Error parsing XML from {url}: {str(e)}")
            return None

    def process_sitemap(url):
        """
        Recursively process sitemap and its children
        """
        if url in processed_urls:
            return

        processed_urls.add(url)
        logger.info(f"Processing sitemap: {url}")

        root = fetch_and_parse_sitemap(url)
        if root is None:
            return

        # Define namespace mapping
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        # Open CSV in append mode
        with open(output_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Process nested sitemaps
            for sitemap in root.findall(".//sm:sitemap", ns):
                loc = sitemap.find("sm:loc", ns)
                lastmod = sitemap.find("sm:lastmod", ns)

                if loc is not None:
                    child_url = loc.text
                    process_sitemap(child_url)

            # Process individual URLs
            for url in root.findall(".//sm:url", ns):
                loc = url.find("sm:loc", ns)
                lastmod = url.find("sm:lastmod", ns)

                if loc is not None:
                    writer.writerow(
                        [
                            loc.text,
                            lastmod.text if lastmod is not None else "",
                            url,
                            datetime.now().isoformat(),
                        ]
                    )

    # Start processing from the root sitemap
    process_sitemap(sitemap_url)
    logger.info(f"Completed! Results saved to {output_file}")


# Example usage:
if __name__ == "__main__":
    root_sitemap = "https://www.practo.com/sitemap.xml"
    extract_sitemap_links(root_sitemap)
