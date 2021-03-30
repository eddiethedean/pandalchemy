import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pandalchemy",
    license='MIT',
    version='0.1.14',
    author="Odos Matthews",
    author_email="odosmatthews@gmail.com",
    description="A package that integrates pandas and sqlalchemy.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eddiethedean/pandalchemy",
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'sqlalchemy==1.3.18',
        'sqlalchemy-migrate',
        'numpy',
        'tabulate'
    ],
    entry_points={
        'console_scripts': [
            'pandalchemy = pandalchemy.cli:main',
        ]
    }
)