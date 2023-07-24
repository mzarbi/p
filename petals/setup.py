from setuptools import setup, find_packages

setup(
    name='petals',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'click',
        'asyncio',
        'xmltodict',
        's3fs'
    ],
    entry_points={
        'console_scripts': [
            'start-petals = scripts.start_petals:start_server',
        ],
    },
    author='Mohamed Zied El Arbi',
    author_email='medzied.arbi@gmail.com',
    description='A Python package to interact with the Petals server.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
