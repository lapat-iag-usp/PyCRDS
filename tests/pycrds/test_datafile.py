import pytest
import pycrds.datafile as datafile


data_read_files = [("dir", ["dir/file_0.txt"]),
                   ("dir", ["dir/file_0.txt", "dir/subdir/file_1.txt"]),
                   ("dir", ["dir/file_0.txt", "dir/subdir/file_1.txt", "dir/subdir/subdir1/file_2.txt"]),
                   ("dir", []),
                   (""   , [])]

data_input_read_files = [(None, TypeError),
                         (5   , TypeError),
                         (""  , []),
                         ("invalid/Path", [])]

@pytest.fixture(params=data_read_files)
def setup_read_files(tmp_path,request):
    dir_rel = request.param[0]
    filenames_rel = request.param[1]
    dir_name = (tmp_path / dir_rel).__str__() #absolute path
    filenames = []
    for f in filenames_rel:
        parent_file = f[:f.rfind("/")]
        parent_file_abs = tmp_path / parent_file
        parent_file_abs.mkdir(parents=True, exist_ok=True) #create directories
        filename_abs = tmp_path / f
        filename_abs.touch() #create files
        filenames.append(filename_abs.__str__())
    return dir_name, filenames


def test_read_files(setup_read_files):
    dir_name, filenames = setup_read_files
    assert datafile.read_files(dir_name) == filenames

@pytest.mark.parametrize("dir_name, expected", data_input_read_files)
def test_input_read_files(dir_name, expected):
    try:
        assert datafile.read_files(dir_name) == expected
    except expected:
        assert True
