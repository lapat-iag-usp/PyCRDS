from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='pycrds',
    version='0.0.1',
    author='Marcia Marques, Daniel Castelo',
    author_email='marcia.marques@alumni.usp.br',
    description='A library to CRDS (cavity ring-down spectroscopy) data processing',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/marcia-marques/PyCRDS',
    py_modules=["datafile", "flags", "graphs"],
    package_dir={'': 'src'},
    install_requires=[
        "numpy >= 1.19",
        "pandas >= 1.1",
        "bokeh >= 2.2",
        "matplotlib >= 3.2",
        "tqdm >= 4.48",
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.7',
    ],
)
