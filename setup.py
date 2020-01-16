from distutils.core import setup
from setuptools import find_packages

setup(
    name='ieUtils',  # How you named your package folder
    packages=find_packages(exclude=["ie_utils.test"]),  # Chose the same as "name"
    author='Sinisa Derasevic',  # Type in your name
    author_email='sinishadj@gmail.com',  # Type in your E-Mail
    install_requires=[
        'boto3',
        'sentry-sdk==0.9.5'
    ]
)
