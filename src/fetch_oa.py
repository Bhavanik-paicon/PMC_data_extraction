import glob
import os
import pathlib
import subprocess
import shutil
from tqdm import tqdm
import logging

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a console handler for logging
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

# Define the logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - \33[32m%(message)s\033[0m')
console_handler.setFormatter(formatter)

from data import OA_LINKS  # Links to PMC Open Access datasets
from args import parse_args_oa  # Argument parser for command-line inputs
from parser import get_volume_info  # Function to extract volume information from XML
from utils import read_jsonl, write_jsonl  # Functions for reading and writing JSONL files

def provide_extraction_dir():
    """Ensures the extraction directory exists and is empty.
    
    Raises:
        Exception: If the directory is not empty and deletion is not confirmed.
    """
    # Create extraction directory if it doesn't exist
    if not os.path.exists(args.extraction_dir):
        os.makedirs(args.extraction_dir, 0o755)
    # Check if the directory is empty, or delete contents if requested
    elif len(os.listdir(args.extraction_dir)) > 0 and not args.keep_archives:
        if not args.delete_extraction_dir:
            raise Exception('The extraction directory {0} is not empty. '
                            'Please pass -d to confirm deletion of its contents.')

        # Delete existing files and directories in the extraction directory
        files = glob.glob(os.path.join(args.extraction_dir, '*'))
        for f in files:
            if os.path.isdir(f):
                shutil.rmtree(f, True)
            else:
                os.remove(f)

def extract_archive(archive_path, target_dir):
    """Extracts a tar.gz archive to the specified target directory.
    
    Args:
        archive_path (str): Path to the archive file.
        target_dir (str): Directory to extract the contents into.
    """
    subprocess.call(['tar', 'zxf', archive_path, '-C', target_dir])

def download_archive(volumes, extract=True):
    """Downloads and extracts archives for specified volumes from PMC.
    
    Args:
        volumes (list): List of volume IDs to download.
        extract (bool): Flag to indicate whether to extract archives after downloading.
    """
    logger.info('Volumes to download: %s' % volumes)

    for volume_id in volumes:
        volume = 'PMC00%dxxxxxx' % volume_id  # Format volume ID
        csv_url = OA_LINKS[volume]['csv_url']  # Get CSV URL from links
        tar_url = OA_LINKS[volume]['tar_url']  # Get tar URL from links
        logger.info(csv_url)
        logger.info(tar_url)

        # Download CSV and tar files
        subprocess.call(['wget', '-nc', '-nd', '-c', '-q', '-P', f'{args.extraction_dir}/{volume}', csv_url])
        subprocess.call(['wget', '-nc', '-nd', '-c', '-q', '-P', f'{args.extraction_dir}/{volume}', tar_url])

        # Check if the archive has already been extracted
        if not pathlib.Path(f'{args.extraction_dir}/{volume}/{volume}').exists():
            logger.info('Extracting %s' % volume)
            extract_archive(
                archive_path=f'{args.extraction_dir}/{volume}/{tar_url.split("/")[-1]}',
                target_dir=f'{args.extraction_dir}/{volume}'
            )
            logger.info('%s Done', volume)
        else:
            logger.info('%s already exists', volume)

def dowload_media(volume_info):
    """Downloads media files associated with the specified volume information.
    
    Args:
        volume_info (list): List of dictionaries containing media URLs and names.
    """
    # Create a directory for downloaded figures
    figures_dir = f'{args.extraction_dir}/figures'
    if not os.path.exists(figures_dir):
        os.makedirs(figures_dir, 0o755)

    # Download each media file
    for obj in tqdm(volume_info, desc='Downloading media'):
        media_url = obj['media_url']
        media_name = obj['media_name']
        file_path = f'{figures_dir}/{media_name}'

        # Attempt to download the media file
        subprocess.call(['wget', '-nc', '-nd', '-c', '-q', '-P', file_path, media_url])
        if not os.path.exists(file_path):
            raise RuntimeError('Download failed. Use the following command to check connection: '
                               'wget https://www.pmc.ncbi.nlm.nih.gov/articles/PMC539052/figure/pmed.0010066.t003.jpg')

if __name__ == '__main__':
    # Check if wget is available
    if not shutil.which("wget"):
        print("wget not found, please install wget and put it on your PATH")
        exit(-1)

    args = parse_args_oa()  # Parse command-line arguments
    download_archive(volumes=args.volumes)  # Download specified archives

    # Check if volume info already exists
    save_name = ''.join([str(volume_id) for volume_id in args.volumes])
    volume_info_path = f'{args.extraction_dir}/{save_name}.jsonl'
    if not os.path.exists(volume_info_path):
        # Extract volume information from XML files
        logger.info('Extracting Volume INFO')
        volume_info = get_volume_info(
            volumes=args.volumes,
            extraction_dir=args.extraction_dir
        )

        # Save volume information in JSONL format
        logger.info('Saving Volume INFO')
        write_jsonl(
            data_list=volume_info,
            save_path=volume_info_path
        )
        logger.info('Saved')
    else:
        volume_info = read_jsonl(file_path=volume_info_path)  # Load existing volume info

    dowload_media(volume_info)  # Download media files
    logger.info('Done')  # Indicate completion
