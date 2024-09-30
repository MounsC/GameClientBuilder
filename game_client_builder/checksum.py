import hashlib
import os
import json
import asyncio


async def calculate_checksum(file_path: str, base_directory: str) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _calculate_checksum, file_path, base_directory)


def _calculate_checksum(file_path: str, base_directory: str) -> dict:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    relative_path = os.path.relpath(file_path, base_directory).replace(os.sep, '/')

    return {
        "Filename": os.path.basename(file_path),
        "Path": relative_path,
        "SHA": sha256_hash.hexdigest().upper(),
    }


async def generate_checksums_for_directory(directory: str, output_directory: str):
    """
    Args:
        directory (str): The directory to traverse to generate checksums.
        output_directory (str): The directory where the JSON file of checksums will be saved.
    """
    tasks = []

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            tasks.append(calculate_checksum(file_path, directory))

    checksums = await asyncio.gather(*tasks)

    output_file = os.path.join(output_directory, "checksums.json")
    with open(output_file, 'w') as f:
        json.dump(checksums, f, indent=4)

    print(f"Checksums saved in: {output_file}")
