from setuptools import setup, find_packages

setup(
    name='trustpipe',
    version='0.1.0',
    description='Task orchestratition for data collection and curation',
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
        'gitpython==3.1',
        'omegaconf==2.3.0',
        'python-slugify==8.0.1',
        ]
)
