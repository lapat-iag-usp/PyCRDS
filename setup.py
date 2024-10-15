from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='pycrds',
    version='0.0.1',
    description='A library to CRDS (cavity ring-down spectroscopy) data processing',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/lapat-iag-usp/PyCRDS',
    author='Marcia Marques',
    author_email='marcia.marques@alumni.usp.br',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.11',
    ],
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        "numpy >= 1.26.4",
        "pandas >= 2.2.2",
        "xarray >= 2024.6.0",
        "dask >= 2024.5.2",
        "dask-expr >= 1.1.2",
        "netCDF4 >= 1.6.5",
    ],
)
