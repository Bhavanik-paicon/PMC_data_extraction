import codecs
import jsonlines
import pandas as pd
import pathlib
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from utils import write_jsonl  # Function to write data in JSONL format

def get_img_url(PMC_ID, graphic):
    """Constructs the URL for an image from its PMC ID and graphic identifier.

    Args:
        PMC_ID (str): The PMC ID of the article.
        graphic (str): The graphic identifier for the image.

    Returns:
        str: The complete URL to the image.
    """
    return f'https://www.ncbi.nlm.nih.gov/pmc/articles/{PMC_ID}/figure/{graphic}/'

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

def parse_xml(xml_path):
    """Parses an XML file to extract image information, including image URLs, captions, and DOIs.

    Args:
        xml_path (str): Path to the XML file to parse.

    Returns:
        list: A list of dictionaries containing extracted image data (PMC_ID, media_id, caption, media_url, etc.).
    """
    item_info = []

    try:
        with codecs.open(xml_path, encoding='utf-8') as f:
            document = f.read()
        soup = BeautifulSoup(document, 'lxml')

        # Extract PMC_ID from xml_path
        if isinstance(xml_path, pathlib.Path):
            xml_path = str(xml_path)
        PMC_ID = xml_path.split('/')[-1].strip('.xml')

        # Extract the DOI
        doi_tag = soup.find('article-id', attrs={'pub-id-type': 'doi'})
        doi = doi_tag.get_text() if doi_tag else 'No DOI found'

        # Find all figure elements in the XML that contain images
        figs = soup.find_all(name='fig')
        for fig in figs:
            media_id = fig.attrs.get('id')

            # Extract the image URL
            if fig.graphic:
                graphic = fig.graphic.attrs['xlink:href']
                graphic_fixed = graphic.rsplit('.', 1)
                if len(graphic_fixed) == 2:
                    graphic_fixed = f"{graphic_fixed[0].replace('.', '-')}-{graphic_fixed[1]}"
                else:
                    graphic_fixed = graphic_fixed[0]
                media_webpage_url = get_img_url(PMC_ID, media_id)
                direct_img_url = get_direct_image_url(media_webpage_url)
                media_name = f'{PMC_ID}_{media_id}.jpg'  # Assuming JPG images

                # Extract the caption
                caption = fig.caption.get_text() if fig.caption else ''

                # Append image data to the list
                item_info.append({
                    'PMC_ID': PMC_ID,
                    'media_id': media_id,
                    'caption': caption,
                    'media_webpage_url': media_webpage_url,
                    'Image_URL':direct_img_url,
                    'media_name': media_name,
                    'doi': doi
                })
            else:
                # Skip non-image media
                continue

    except Exception as e:
        print(f"Error parsing XML file {xml_path}: {e}")

    return item_info

def get_volume_info(volumes, extraction_dir: pathlib.Path):
    """Extracts image information from specified volumes and processes each XML file in the volume.

    Args:
        volumes (list): List of volume IDs to process.
        extraction_dir (pathlib.Path): Directory where the volume XML files are stored.

    Returns:
        list: A list of dictionaries containing media information from all volumes.
    """
    info = []

    try:
        if not isinstance(extraction_dir, pathlib.Path):
            extraction_dir = pathlib.Path(extraction_dir)

        for volume_id in volumes:
            volume = f'PMC00{volume_id}xxxxxx'
            file_name = f'oa_comm_xml.{volume}.baseline.2024-06-18.filelist.csv'
            file_path = extraction_dir / volume / file_name

            # Read the CSV file to get a list of article XMLs
            df = pd.read_csv(file_path, sep=',')

            # Process each XML file
            for idx in tqdm(range(len(df)), desc='Parsing XML'):
                xml_path = extraction_dir / volume / df.loc[idx, 'Article File']
                item_info = parse_xml(xml_path)
                info += item_info

    except Exception as e:
        print(f"Error extracting volume info: {e}")

    return info

if __name__ == '__main__':
    print('\033[32mParse PMC documents\033[0m')

    # Example of extracting volume information from specified volumes
    volume_info = get_volume_info(
        volumes=[0],  # Example volume ID
        extraction_dir=pathlib.Path('./PMC_OA')
    )

    print(f'Number of figures in volumes: {len(volume_info)}')

    # Save extracted media information to a JSONL file
    try:
        write_jsonl(
            data_list=volume_info,
            save_path='./volume0.jsonl'
        )
        print("JSONL file saved successfully.")
    except Exception as e:
        print(f"Error saving JSONL file: {e}")
