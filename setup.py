import os
import sys
import glob

#from distutils.core import setup, Extension
from setuptools import setup, find_packages, Extension, Feature


COVERAGE_TEST=bool(os.getenv('COVERAGE_TEST', ''))

if sys.version_info < (2, 5):
    raise ValueError("Python older than 2.5 is not supported")

if sys.version_info >= (3, 0):
    raise ValueError("Python 3 is not supported")

ext_kwargs = {}
if COVERAGE_TEST:
    ext_kwargs.update( dict(
        extra_compile_args=["--cover","-DCOVERAGE_TEST"],
        extra_link_args=["--cover"],
    ) )

extensions = [Extension('ziutek._qlist',
                sources=['ziutek/qlist_p.c', 'ziutek/qlist.c'],
                define_macros=dict(RELEASE=1,).items(),
                depends=(glob.glob('ziutek/*.h') + ['setup.py']),
                **ext_kwargs
            )]

setup(name='ziutek',
    description = "Ziutek, the fast and simple full-text search engine.",
    version='0.1',
    license='BSD License',
    author='Marek Majkowski',
    url = 'http://code.google.com/p/ziutek',
    packages=find_packages('.'),
    ext_modules=extensions,
    platforms='Linux',
    test_suite="ziutek.tests.all_tests_suite",
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: C',
        'Operating System :: POSIX',
        'License :: OSI Approved :: BSD License',
        ],
    package_data = {'': ['plugin_qlist.c', 'qlist.c']},
    )
