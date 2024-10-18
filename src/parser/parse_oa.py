import codecs
import jsonlines
import pandas as pd
import pathlib
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
    img_url = f'https://www.pmc.ncbi.nlm.nih.gov/articles/{PMC_ID}/figure/{graphic}.jpg'
    return img_url

def get_video_url(PMC_ID, media):
    """Constructs the URL for a video from its PMC ID and media identifier.
    
    Args:
        PMC_ID (str): The PMC ID of the article.
        media (str): The media identifier for the video.
    
    Returns:
        str: The complete URL to the video.
    """
    mov_url = f'https://www.pmc.ncbi.nlm.nih.gov/articles/{PMC_ID}/figure/{media}'
    return mov_url

def parse_xml(xml_path):
    """Parses an XML file to extract media information.
    
    Args:
        xml_path (str or pathlib.Path): The path to the XML file to be parsed.
    
    Returns:
        list: A list of dictionaries, each containing:
            - 'PMC_ID': The PMC ID of the article.
            - 'media_id': The ID of the media item.
            - 'caption': The caption for the media item (if available).
            - 'media_url': The URL to the media item.
            - 'media_name': The file name for the media item.
    
    Raises:
        RuntimeError: If an unsupported media type is found or if parsing fails.
    """
    with codecs.open(xml_path, encoding='utf-8') as f:
        document = f.read()
    soup = BeautifulSoup(document, 'lxml')

    # Extract PMC_ID from xml_path
    if isinstance(xml_path, pathlib.Path):
        xml_path = str(xml_path)
    PMC_ID = xml_path.split('/')[-1].strip('.xml')
    
    item_info = []

    # Find all figure elements in the XML
    figs = soup.find_all(name='fig')
    for fig in figs:
        media_id = fig.attrs['id']

        if fig.graphic:
            graphic = fig.graphic.attrs['xlink:href']
            media_url = get_img_url(PMC_ID, graphic)
            file_extension = media_url.split('.')[-1]  # e.g., .jpg
            media_name = f'{PMC_ID}_{media_id}.jpg'  # Assign .jpg manually
        elif fig.media:
            media = fig.media.attrs['xlink:href']
            media_url = get_video_url(PMC_ID, media)
            file_extension = media_url.split('.')[-1]  # e.g., .mov, .dcr, .avi
            media_name = f'{PMC_ID}_{media_id}.{file_extension}'
        else:
            raise RuntimeError(f'Error occurred when parsing XML figs: {xml_path}')

        # Check for unsupported media types
        if file_extension not in ['mov', 'jpg', 'dcr', 'avi', 'mpeg']:
            raise RuntimeError(f'{xml_path} contains unsupported media type: {media_name}, {media_url}')

        # Handle missing captions
        caption = fig.caption.get_text() if fig.caption else ''

        # Append extracted information to the list
        item_info.append({
            'PMC_ID': PMC_ID,
            'media_id': media_id,
            'caption': caption,
            'media_url': media_url,
            'media_name': media_name
        })

    return item_info

def get_volume_info(volumes, extraction_dir: pathlib.Path):
    """Extracts media information from specified volumes.
    
    Args:
        volumes (list): A list of volume IDs to extract.
        extraction_dir (pathlib.Path): The directory where the volume XML files are located.
    
    Returns:
        list: A list of dictionaries containing media information from all specified volumes.
    """
    if not isinstance(extraction_dir, pathlib.Path):
        extraction_dir = pathlib.Path(extraction_dir)
    info = []
    
    # Process each volume ID
    for volume_id in volumes:
        volume = f'PMC00{volume_id}xxxxxx'  # Format volume ID
        file_name = f'oa_comm_xml.{volume}.baseline.2024-06-18.filelist.csv'
        file_path = extraction_dir / volume / file_name #"~/Desktop/PMC/"+ file_name

        # Read the file list CSV into a DataFrame
        df = pd.read_csv(file_path, sep=',')

        # Parse each XML file listed in the CSV
        for idx in tqdm(range(len(df)), desc='Parsing XML'):
            xml_path = extraction_dir / volume / df.loc[idx, 'Article File'] #"~/Desktop/PMC/oa_comm_xml.PMC000xxxxxx.baseline.2024-06-18/" + df.loc[idx, 'Article File']
            item_info = parse_xml(xml_path)  # Parse XML file to extract media info
            info += item_info  # Append extracted info to the list
    
    return info

if __name__ == '__main__':
    print('\033[32mParse PMC documents\033[0m')

    # Extract volume information from specified volumes
    volume_info = get_volume_info(
        volumes=[0],  # Example volume ID
        extraction_dir=pathlib.Path('./PMC_OA')
    )
    print(f'Number of figures in volumes: {len(volume_info)}')
    
    # Save extracted media information to a JSONL file
    write_jsonl(
        data_list=volume_info,
        save_path='./volume0.jsonl'
    )
