import pytest
import pycrds.datafile as datafile
import pandas as pd
import shutil
from os.path import dirname, abspath
from datetime import datetime


"""
Test function read_files
"""

""" Data for setup_read_files functions. E.g.: [(dir_name, filenames)]"""
data_read_files = [("dir", ["dir/file_0.dat"]),
                   ("dir", ["dir/file_0.dat", "dir/subdir/file_1.dat"]),
                   ("dir", ["dir/file_0.dat", "dir/subdir/file_1.dat", "dir/subdir/subdir1/file_2.dat"]),
                   ("dir", []),
                   (""   , [])]

""" Data for test_input_read_files functions. E.g.: [(dir_name, ext, expected)] """
data_input_read_files = [(None, ".dat", TypeError),
                         (5   , ".dat", TypeError),
                         (""  , None  , TypeError),
                         (""  , ".dat", []),
                         ("invalid/Path", ".dat", []),
                         (dirname(dirname(abspath(__file__))) + "/data/G2301/DataLog_User", ".dat",
 [dirname(dirname(abspath(__file__))) + "/data/G2301/DataLog_User\\2021\\01\\01\\CFADS2502-20210101-235440Z-DataLog_User.dat",
  dirname(dirname(abspath(__file__))) + "/data/G2301/DataLog_User\\2021\\01\\02\\CFADS2502-20210102-005446Z-DataLog_User.dat"]),
                         (dirname(dirname(abspath(__file__))) + "/data/G2301/DataLog_User", ".csv", []),
                         (dirname(dirname(abspath(__file__))) + "/data/G2301/DataLog_User", ".txt", 
 [dirname(dirname(abspath(__file__))) + "/data/G2301/DataLog_User\\2021\\01\\01\\CFADS2502-20210101-235440Z-DataLog_User_Test_Extension.txt"])]

@pytest.fixture(params=data_read_files)
def setup_read_files(tmp_path, request):
    """ Creating setup of temporary directories and files for test read_files function. """
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
    """ Test datafile.read_files function with temporary directories"""
    dir_name, filenames = setup_read_files
    assert datafile.read_files(dir_name) == filenames

@pytest.mark.parametrize("dir_name, ext, expected", data_input_read_files)
def test_input_read_files(dir_name, ext, expected):
    """ Test inputs of datafile.read_files function with temporary directories"""
    try:
        assert datafile.read_files(dir_name, ext) == expected
    except expected:
        assert True


"""
Test function read_data
"""

dir_name = dirname(dirname(abspath(__file__))) + "/data/G2301/DataLog_User"
mycols = ["DATE", "TIME", "FRAC_DAYS_SINCE_JAN1", "FRAC_HRS_SINCE_JAN1", "JULIAN_DAYS",
 "EPOCH_TIME", "ALARM_STATUS", "INST_STATUS", "CavityPressure", "CavityTemp", "DasTemp",
 "EtalonTemp", "WarmBoxTemp", "species", "MPVPosition", "OutletValve", "solenoid_valves",
 "CH4", "CH4_dry", "CO2", "CO2_dry", "H2O", "h2o_reported", "wlm1_offset", "wlm2_offset", "wlm4_offset"]

def setup_df_from_csv():
    """ Creating setup of dataframe from csv file."""
    df_expected = pd.read_csv(dirname(dirname(abspath(__file__))) + "/data/G2301/data.csv").set_index('DATE_TIME')
    df_expected.index = pd.to_datetime(df_expected.index)
    return df_expected

""" Data for test_read_data functions. E.g.: [(dir_name, mycols, expected)]"""
data_read_data = [(dir_name, mycols, setup_df_from_csv()),
                  (""      , [], ValueError),
                  (dir_name, [], ValueError),
                  (""  , mycols, ValueError),
                  (dir_name, None, setup_df_from_csv()),
                  (dir_name, ["DATE", "TIME", "CO2_dry"], (setup_df_from_csv())[['CO2_dry']])]


@pytest.mark.parametrize("dir_name, mycols, expected", data_read_data)
def test_read_data(dir_name, mycols, expected):
    """ Test datafile.read_data function"""
    try:
        df = datafile.read_data(dir_name, mycols)
        pd.testing.assert_frame_equal(df, expected)
    except expected:
        assert True


"""
Test function save_24h
"""

df = pd.DataFrame(data={"col_A":[1,2], "col_B":["X", "Y"]}, index=[datetime(2021, 1, 28, 23, 55, 58), datetime(2021, 1, 28, 23, 55, 59)])
df1 = pd.DataFrame(data={"col_A":[1,2], "col_B":["X", "Y"]})
df2 = pd.DataFrame(data={"col_A":[1,2,1,2], "col_B":["X","Y","X","Y"]}, index=[datetime(2021, 1, 28, 23, 55, 58), datetime(2021, 1, 28, 23, 55, 59), datetime(2021, 1, 28, 23, 55, 58), datetime(2021, 1, 28, 23, 55, 59)])

""" Data for test_save_24h functions. E.g.: [(df, path, file_id, level, expected)]"""
data_save_24h = [(df, "tmp_path/dir", "", "", df),
                (None, "tmp_path", "", "", TypeError),
                (df, None, "", "", TypeError),
                (df, "tmp_path", None, "", TypeError),
                (df, "tmp_path", "", None, TypeError),
                (df1, "tmp_path", "", None, TypeError)]

""" Data for test_save_24h_append functions. E.g.: [(df, path, file_id, level, expected)]"""
data_save_24h_append = [(df, "tests/data/datafile/save_24", "TEST01", "0", df2)]

@pytest.fixture(params=data_save_24h_append)
def prepare_tmp_data_24h_append(tmp_path, request):
    """ Create temporary file to be modified by datafile.save_24h function"""
    df, path, file_id, level, expected = request.param
    path = tmp_path / path
    full_path = path / "2021/01"
    full_path.mkdir(parents=True, exist_ok=True)
    shutil.copy("./tests/data/datafile/save_24/2021/01/TEST01-20210128-Z-DataLog_User_0.csv", full_path)
    return df, path.__str__(), file_id, level, expected

@pytest.mark.parametrize("df, path, file_id, level, expected", data_save_24h)
def test_save_24h(df, path, file_id, level, expected, tmp_path):
    """ Test datafile.save_24h function. Save new csv file."""
    if isinstance(path, str):
        path = path.replace("tmp_path", tmp_path.__str__())
    save_24h(df, path, file_id, level, expected)

def test_save_24h_append(prepare_tmp_data_24h_append):
    """ Test datafile.save_24h function. Modify existing csv file."""
    df, path, file_id, level, expected = prepare_tmp_data_24h_append
    save_24h(df, path, file_id, level, expected)

def save_24h(df, path, file_id, level, expected):
    """ Test datafile.save_24h function"""
    try:
        datafile.save_24h(df, path, file_id, level)
    except expected:
        assert True
        return
    file_name = path + '/2021/01' +\
            '/' + file_id + '-' +\
            df.index[0].strftime('%Y%m%d') + '-' + \
            'Z-DataLog_User_' + level + '.csv'
    df_test = pd.read_csv(file_name, index_col=0, parse_dates=True)
    pd.testing.assert_frame_equal(df_test, expected)