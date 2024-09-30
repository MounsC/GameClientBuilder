import os
import tarfile
import shutil
import asyncio


async def compress_directory_with_tar(directory: str, output_tar: str):
    """
    Args:
        directory (str): Directory to compress.
        output_tar (str): Path of the TAR.gz file to generate.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _compress_with_tar, directory, output_tar)
    print(f"Compression of the directory {directory} completed in {output_tar}.tar.gz")


def _compress_with_tar(directory: str, output_tar: str):
    with tarfile.open(f"{output_tar}.tar.gz", "w:gz") as tar:
        tar.add(directory, arcname=os.path.basename(directory))


async def compress_files_in_chunks_with_tar(directory: str, chunk_size_mb: int, output_directory: str):
    """
    Args:
        directory (str): Directory containing the files to compress.
        chunk_size_mb (int): Maximum size of a chunk in megabytes.
        output_directory (str): Output directory for the TAR.gz files.
    """
    chunk_size_bytes = chunk_size_mb * 1024 * 1024
    files = []

    for root, _, file_names in os.walk(directory):
        for file_name in file_names:
            file_path = os.path.join(root, file_name)
            files.append(file_path)

    chunk_index = 1
    current_chunk_size = 0
    chunk_dir = os.path.join(output_directory, f"chunk_{chunk_index}")
    os.makedirs(chunk_dir, exist_ok=True)

    for file in files:
        file_size = os.path.getsize(file)

        if current_chunk_size + file_size > chunk_size_bytes:
            output_tar = os.path.join(output_directory, f"chunk_{chunk_index}")
            await compress_directory_with_tar(chunk_dir, output_tar)
            shutil.rmtree(chunk_dir)
            chunk_index += 1
            current_chunk_size = 0
            chunk_dir = os.path.join(output_directory, f"chunk_{chunk_index}")
            os.makedirs(chunk_dir, exist_ok=True)

        shutil.copy(file, chunk_dir)
        current_chunk_size += file_size

    if current_chunk_size > 0:
        output_tar = os.path.join(output_directory, f"chunk_{chunk_index}")
        await compress_directory_with_tar(chunk_dir, output_tar)
        shutil.rmtree(chunk_dir)
