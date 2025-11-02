from setuptools import setup, find_packages

setup(
    name='taf_august',
    version='1.0.0',
    author='Sreenivasulu Kattubadi',
    description='Test Automation Framework for Data Quality Validation',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pytest',
        'pyyaml',
        'pandas',
        'pyspark',
        'snowflake-connector-python'
    ],
)