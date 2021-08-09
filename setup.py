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
    url='https://github.com/marcia-marques/PyCRDS',
    author='Marcia Marques, Daniel Castelo',
    author_email='marcia.marques@alumni.usp.br',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.7',
    ],
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        "numpy >= 1.19",
        "pandas >= 1.1",
        "bokeh >= 2.2",
        "matplotlib >= 3.2",
        "tqdm >= 4.48",
    ],
)
