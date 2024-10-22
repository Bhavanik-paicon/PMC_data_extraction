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
    """Ensures the extraction directory exists and handles cases where the directory is not empty.
    
    Raises:
        Exception: If the directory is not empty and deletion is not confirmed with the `-d` flag.
    """
    try:
        # Check if the extraction directory exists
        if not os.path.exists(args.extraction_dir):
            os.makedirs(args.extraction_dir, 0o755)

        # If the directory exists and is not empty
        elif len(os.listdir(args.extraction_dir)) > 0:
            if args.keep_archives:
                # Keep existing files, do nothing
                logger.info(f"Keeping existing files in {args.extraction_dir}.")
            elif args.delete_extraction_dir:
                # Delete existing files and directories if `-d` flag is passed
                logger.info(f"Deleting existing contents in {args.extraction_dir} as per -d flag.")
                files = glob.glob(os.path.join(args.extraction_dir, '*'))
                for f in files:
                    if os.path.isdir(f):
                        shutil.rmtree(f, True)
                    else:
                        os.remove(f)
            else:
                # Raise an exception if neither flag is passed
                raise Exception(f'The extraction directory {args.extraction_dir} is not empty. '
                                'Please pass -d to confirm deletion of its contents, or --keep-archives to preserve them.')

    except Exception as e:
        logger.error(f"Error in provide_extraction_dir: {e}")
        raise


def extract_archive(archive_path, target_dir):
    """Extracts a tar.gz archive to the specified target directory.
    
    Args:
        archive_path (str): Path to the archive file.
        target_dir (str): Directory to extract the contents into.
    """
    try:
        subprocess.call(['tar', 'zxf', archive_path, '-C', target_dir])
    except Exception as e:
        logger.error(f"Error extracting archive {archive_path}: {e}")

def download_archive(volumes, extract=True):
    """Downloads and optionally extracts archives for the specified volumes from PMC.
    
    Args:
        volumes (list): List of volume IDs to download.
        extract (bool): Flag indicating whether to extract archives after downloading.
    """
    logger.info('Volumes to download: %s' % volumes)

    for volume_id in volumes:
        volume = 'PMC00%dxxxxxx' % volume_id
        csv_url = OA_LINKS[volume]['csv_url']
        tar_url = OA_LINKS[volume]['tar_url']
        logger.info(f"CSV URL: {csv_url}")
        logger.info(f"Tar URL: {tar_url}")

        # Download CSV and tar files
        try:
            subprocess.call(['wget', '-nc', '-nd', '-c', '-q', '-P', f'{args.extraction_dir}/{volume}', csv_url])
            subprocess.call(['wget', '-nc', '-nd', '-c', '-q', '-P', f'{args.extraction_dir}/{volume}', tar_url])

            # Extract the archive if it hasn't been extracted yet
            if not pathlib.Path(f'{args.extraction_dir}/{volume}/{volume}').exists():
                logger.info(f'Extracting {volume}')
                extract_archive(f'{args.extraction_dir}/{volume}/{tar_url.split("/")[-1]}', f'{args.extraction_dir}/{volume}')
                logger.info(f'Extraction complete for {volume}')
            else:
                logger.info(f'{volume} already exists')

        except Exception as e:
            logger.error(f"Error downloading or extracting archive for volume {volume_id}: {e}")

def download_media(volume_info):
    """Downloads media files (images) associated with the specified volume information.
    
    Args:
        volume_info (list): List of dictionaries containing media URLs and names.
    """
    figures_dir = f'{args.extraction_dir}/figures'
    try:
        if not os.path.exists(figures_dir):
            os.makedirs(figures_dir, 0o755)

        # Download each media file
        for obj in tqdm(volume_info, desc='Downloading media'):
            direct_img_url = obj['Image_URL']
            media_name = obj['media_name']
            file_path = f'{figures_dir}/{media_name}'

            try:
                subprocess.call(['wget', '-nc', '-nd', '-c', '-q', '-P', figures_dir, direct_img_url])
                if not os.path.exists(file_path):
                    raise RuntimeError(f"Download failed for {media_name}. Check connection using: "
                                       f"wget {direct_img_url}")
            except Exception as e:
                logger.error(f"Error downloading {media_name}: {e}")

    except Exception as e:
        logger.error(f"Error in download_media: {e}")

if __name__ == '__main__':
    # Check if wget is available
    if not shutil.which("wget"):
        logger.error("wget not found, please install wget and put it on your PATH")
        exit(-1)

    try:
        args = parse_args_oa()
        provide_extraction_dir()

        # Download and extract archives for specified volumes
        download_archive(volumes=args.volumes)

        # Load or extract volume information
        save_name = ''.join([str(volume_id) for volume_id in args.volumes])
        volume_info_path = f'{args.extraction_dir}/{save_name}.jsonl'

        if not os.path.exists(volume_info_path):
            logger.info('Extracting Volume INFO')
            volume_info = get_volume_info(volumes=args.volumes, extraction_dir=args.extraction_dir)
            write_jsonl(volume_info, volume_info_path)
            logger.info('Volume INFO saved.')
        else:
            volume_info = read_jsonl(volume_info_path)

        # Download the media files
        download_media(volume_info)
        logger.info('Media download complete.')

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
