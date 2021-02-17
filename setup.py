from setuptools import setup

setup(
    name='pycrds',
    version='0.0.1',
    description='A library to CRDS (cavity ring-down spectroscopy) data processing',
    py_modules=["datafile", "flags", "graphs"],
    package_dir={'': 'src'},
    install_requires=[
        "numpy >= 1.19",
        "pandas >= 1.1",
        "bokeh >= 2.2",
        "matplotlib >= 3.2",
        "tqdm >= 4.48",
    ],
)
