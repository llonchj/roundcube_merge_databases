import os
import sys
import string
from setuptools import setup, find_packages

setup(
    name='roundcube_merge_databases',
    version='0.1.3',
    author='Jordi Llonch',
    author_email='llonchj@gmail.com',
    url='https://github.com/nitidum/roundcube_merge_databases.git',
    description='Merge roundcube users/identities/contacts '
                'from one database into another handling id\'s',
    long_description=open('README.md', 'r').read(),
    packages=find_packages(),
    zip_safe=False,
    install_requires=open("requisites.txt").read().split("\n"),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'roundcube_merge_databases = roundcube_merge_databases:main',
        ]
    },
)
