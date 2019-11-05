from setuptools import setup, find_packages

setup(
    name='ieUtils',
    packages=find_packages(exclude=["utils.test"]),
    author='sinisa',
    install_requires=[
        'boto3==1.9.43',
        'pycountry==19.7.15',
        'sentry-sdk==0.9.5'
    ]
)
