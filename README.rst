========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/python-bamboo/badge/?style=flat
    :target: https://readthedocs.org/projects/python-bamboo
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.org/eddiethedean/python-bamboo.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/eddiethedean/python-bamboo

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/eddiethedean/python-bamboo?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/eddiethedean/python-bamboo

.. |requires| image:: https://requires.io/github/eddiethedean/python-bamboo/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/eddiethedean/python-bamboo/requirements/?branch=master

.. |codecov| image:: https://codecov.io/gh/eddiethedean/python-bamboo/branch/master/graphs/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/eddiethedean/python-bamboo

.. |version| image:: https://img.shields.io/pypi/v/bamboo.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/bamboo

.. |wheel| image:: https://img.shields.io/pypi/wheel/bamboo.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/bamboo

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/bamboo.svg
    :alt: Supported versions
    :target: https://pypi.org/project/bamboo

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/bamboo.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/bamboo

.. |commits-since| image:: https://img.shields.io/github/commits-since/eddiethedean/python-bamboo/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/eddiethedean/python-bamboo/compare/v0.0.0...master



.. end-badges

Combines pandas and sqlalchemy to seamlessly allow users to manipulate database tables and create new tables using
pandas DataFrames.

* Free software: MIT license

Installation
============

::

    pip install pandalchemy

You can also install the in-development version with::

    pip install https://github.com/eddiethedean/pandalchemy/archive/master.zip


Documentation
=============


https://pandalchemy.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
