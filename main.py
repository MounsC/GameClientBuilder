import asyncio
from game_client_builder.checksum import generate_checksums_for_directory
from game_client_builder.compression import compress_files_in_chunks_with_tar
from game_client_builder.config import Config
import os


def main():
    print("=== Directory ===")
    root_directory = os.path.dirname(Config.CLIENT_DIRECTORY)

    print("Generating checksums...")
    asyncio.run(generate_checksums_for_directory(Config.CLIENT_DIRECTORY, root_directory))

    print("Compressing files into chunks...")
    asyncio.run(
        compress_files_in_chunks_with_tar(Config.CLIENT_DIRECTORY, Config.CHUNK_SIZE_MB, Config.OUTPUT_DIRECTORY)
    )

if __name__ == "__main__":
    main()
