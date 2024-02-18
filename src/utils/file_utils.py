from typing import List
from pathlib import Path
import logging

class FileUtils:
    logger = logging.getLogger(__name__)

    def __init__(self) -> None:
        raise NotImplementedError("This is a util class and should not be initialized.")

    @classmethod
    def chunk_single_file(cls, filepath: str, chunk_size: int) -> List[str]:
        """Chunk a single file to a sort of chunks.

        Args:
            filepath (str): The path of target file.
            chunk_size (int): The size of each chunk.

        Return:
            List: A list contains the names of chunks.
        """
        if not isinstance(chunk_size, int):
            cls.logger.error(f"The size of chunk must be an Integer, it is {chunk_size} now")
            raise TypeError(f"The size of chunk must be an Integer, it is {chunk_size} now")
        
        orig_path = Path(filepath)
        if not orig_path.exists():
            cls.logger.error(f"File path {filepath} does not exist.")
            raise ValueError(f"File path {filepath} does not exist.")

        chunk_num = 0
        chunks = []

        with open(file=orig_path, mode="rb") as file:
            while chunk := file.read(chunk_size):
                chunk_file_path = orig_path.parent / f"{orig_path.stem}_chunk{chunk_num}{orig_path.suffix}"
                
                cls.logger.info(f"Writing chunk {chunk_num}: {chunk_file_path}...")
                with open(file=chunk_file_path, mode="wb") as chunk_file:
                    chunk_file.write(chunk)
                cls.logger.info(f"Chunk {chunk_num} has been written successfully to {chunk_file_path}.")
                
                chunk_num += 1
                chunks.append(str(chunk_file_path))
        
        cls.logger.info(f"All chunks has been written.\n {chunks}")
        return chunks

    @classmethod
    def merge_chunks(cls, chunks: List[str], output_file_path: str) -> None:
        """Merge multiple chunk files into a single file.

        Args:
            chunks (List[str]): The list of chunk file paths to be merged.
            output_file_path (str): The path of the output file where chunks will be merged.

        """
        with open(output_file_path, 'wb') as output_file:
            for chunk_file_path in chunks:
                with open(chunk_file_path, 'rb') as chunk_file:
                    output_file.write(chunk_file.read())
        
        cls.logger.info(f"All chunks have been merged into {output_file_path}")
