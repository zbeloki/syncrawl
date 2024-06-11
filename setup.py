from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()
    
setup(
    name="syncrawl",
    version="0.1.0",
    author="Zuhaitz Beloki Leitza",
    author_email="zbeloki@gmail.com",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=find_packages(include=[
        "syncrawl", "syncrawl.*",
        "bin", "bin.*",
        "templates", "templates.*",
    ]),
    entry_points={
        'console_scripts': [
            'run-web-server=bin.run_web_server:main',
        ],
    },
    classifiers=[
    ],
    install_requires=[
        "flask==3.0.3",
        "flask_pymongo==2.3.0",
        "lxml==5.2.1",
        "requests==2.31.0",
        "pymongo==4.7.2",
        "pytest==8.2.0",
    ],
)
