import os
from utils.file_utils import FileUtils
import pytest

# Helper function
def generate_test_file(filename: str, size: int):
    with open(filename, 'wb') as f:
        f.write(os.urandom(size))  # Generate a random file with target size

class TestFileUtils:
    @pytest.fixture
    def setup_test_file(self, tmp_path):
        test_file = tmp_path / "test_file.txt"
        generate_test_file(test_file, 1024 * 1024)  # 1MB
        return test_file

    def test_chunk_and_merge_file(self, setup_test_file, tmp_path):
        test_file = setup_test_file
        chunk_size = 1024 * 256  # 256KB
        output_file = tmp_path / "merged_test_file.txt"

        chunks = FileUtils.chunk_single_file(str(test_file), chunk_size)
        assert len(chunks) == 4  # Should have 4 chunks

        # Test merge_chunks
        FileUtils.merge_chunks(chunks, str(output_file))

        # Verify if same as original file
        with open(test_file, 'rb') as original, open(output_file, 'rb') as merged:
            assert original.read() == merged.read(), "The merged file content is not equal to the original file content."