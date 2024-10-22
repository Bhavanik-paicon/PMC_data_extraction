import os
import jsonlines
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm

def get_direct_image_url(page_url):
    """Extracts the direct image URL from a given webpage URL.

    Args:
        page_url (str): The URL of the page containing the image.

    Returns:
        str: The direct image URL if found, otherwise None.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.ncbi.nlm.nih.gov/'
    }

    try:
        response = requests.get(page_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            img_tag = soup.find('img', class_='graphic')
            if img_tag and 'src' in img_tag.attrs:
                img_src = img_tag['src']
                # Check if the URL already starts with "https:" and avoid adding extra "https:"
                if img_src.startswith('https:'):
                    return img_src
                else:
                    return 'https:' + img_src
        else:
            print(f"Failed to retrieve page: {page_url}, Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error accessing page {page_url}: {e}")
        return None


def download_image(url, save_path):
    """Download an image from a URL and save it locally.
    
    Args:
        url (str): The URL of the image to be downloaded.
        save_path (str): The local file path where the image will be saved.
    
    Returns:
        bool: True if the image was downloaded successfully, False otherwise.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.ncbi.nlm.nih.gov/'
    }

    try:
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return True
        else:
            print(f"Failed to download image: {url}, Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def download_images_from_jsonl(jsonl_file, download_dir):
    """Read a JSONL file, extract image URLs, and download the images.
    
    Args:
        jsonl_file (str): Path to the JSONL file containing media information.
        download_dir (str): Directory where images will be saved.
    """
    # Ensure the download directory exists
    os.makedirs(download_dir, exist_ok=True)

    # Read the JSONL file and download images
    with jsonlines.open(jsonl_file) as reader:
        for item in tqdm(reader, desc="Downloading images"):
            media_url = item.get('media_url')
            media_name = item.get('media_name')
            
            if media_url and media_name:
                # Extract direct image URL
                direct_image_url = get_direct_image_url(media_url)
                if direct_image_url:
                    save_path = Path(download_dir) / media_name
                    download_image(direct_image_url, save_path)
                else:
                    print(f"Could not find direct image URL for {media_url}")

if __name__ == '__main__':
    # Specify the path to the JSONL file (e.g., volume0.jsonl)
    jsonl_file = './volume0.jsonl'
    
    # Specify the directory where you want to save the downloaded images
    download_dir = './downloaded_images'
    
    # Start the download process
    download_images_from_jsonl(jsonl_file, download_dir)
    print("Image download process completed.")
