from setuptools import setup, find_packages

setup(
    name='trustpipe',
    version='0.1.0',
    description='Task orchestration for data collection and curation',
    entry_points={
        'console_scripts': [
            'trustpipe = trustpipe.cli:main',
            ],
        },
    packages=find_packages(),    
    install_requires=[
        'sqlalchemy==1.4.46',
        'luigi==3.4',
        'docker==6.1',
        'gitpython==3.1.41',
        'omegaconf==2.3.0',
        'python-slugify==8.0.1',
        'jq==1.6.0',
        'click==8.1.7',
        ]
)
