#!/usr/bin/env python

# Copyright (c) 2015 OpenTable

# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import sys

from mesos_stats import __version__

try:
    from setuptools import setup
    setup  # workaround for pyflakes issue #13
except ImportError:
    from distutils.core import setup

requirements = open('requirements/base.txt').readlines()

setup(
    name='mesos_stats',
    version=__version__,
    author='OpenTable',
    author_email='ssalisbury@opentable.com',
    packages=[
        'mesos_stats',
    ],
    scripts=['bin/mesos_stats'],
    url='http://github.com/opentable/mesos_stats',
    license='LICENSE.txt',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Mesos :: Monitoring :: Stats',
    ],
    description='collect stats from mesos, send to graphite',
    long_description=open('README.md').read() + '\n\n' +
                open('CHANGES.md').read(),
    tests_require=open('requirements/tests.txt').readlines(),
    test_suite='nose.collector',
    install_requires=requirements
)

