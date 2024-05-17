import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name="syncrawl",
    version="0.1.0",
    author="Zuhaitz Beloki Leitza",
    author_email="zbeloki@gmail.com",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=[
        "syncrawl",
    ],
    classifiers=[
    ],
    install_requires=[
        "lxml==5.2.1",
        "requests==2.31.0",
        "pytest==8.2.0",
    ],
)
