from distutils.core import setup
from setuptools import find_packages

setup(
    name='ieUtils',  # How you named your package folder
    packages=find_packages(exclude=["ie_utils.test"]),  # Chose the same as "name"
    version='0.5',  # Start with a small number and increase it with every change you make
    license='MIT',  # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    description='Utility functions',  # Give a short description about your library
    author='Sinisa Derasevic',  # Type in your name
    author_email='sinishadj@gmail.com',  # Type in your E-Mail
    url='https://github.com/sinishadj/ie-utils',  # Provide either the link to your github or to your website
    download_url='https://github.com/sinishadj/ie-utils/archive/v_05.tar.gz',
    keywords=['UTILITIES', 'S3', 'DYNAMO DB'],  # Keywords that define your package best
    install_requires=[
        'boto3==1.9.43',
        'sentry-sdk==0.9.5'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',  # Define that your audience are developers
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',  # Again, pick a license
        'Programming Language :: Python :: 3.6',  # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3.7',
    ],
)