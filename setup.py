import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pandalchemy",
    license='MIT',
    version='0.2.0',
    author="Odos Matthews",
    author_email="odosmatthews@gmail.com",
    description="A package that integrates pandas and sqlalchemy with change tracking and optimized SQL operations.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eddiethedean/pandalchemy",
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
    install_requires=[
        'pandas>=1.5.0',
        'sqlalchemy>=2.0.0',
        'fullmetalalchemy>=0.1.0',
        'transmutation>=0.1.0',
        'numpy>=1.20.0',
        'tabulate>=0.8.0'
    ],
    entry_points={
        'console_scripts': [
            'pandalchemy = pandalchemy.cli:main',
        ]
    }
)