import doctest
import glob
import os
import unittest

DOCTEST_BLACKLIST=[
]

def my_import(modulename):
    mod = __import__(modulename)
    components = modulename.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def mass_import(list_of_modulenames):
    return [my_import(modulename) for modulename in list_of_modulenames]

def list_files(directory, file_mask):
    cwd = os.getcwd()
    os.chdir(directory)
    files = [os.path.split(f)[1].rpartition('.')[0] \
                                                for f in glob.glob(file_mask)]
    os.chdir(cwd)
    return files

def get_module_names(prefix_module_name, directory, file_mask, skip_modules):
    module_names = list_files(directory, file_mask)
    if prefix_module_name and prefix_module_name != "-":
        module_names = [prefix_module_name + "." + mn for mn in module_names]
    module_names = filter(lambda mn: not max([mn.startswith(smn) for smn in skip_modules]) if skip_modules else True, module_names)
    return sorted(module_names)


def additional_tests(suite=None):
    if suite is None:
        suite = unittest.TestSuite()

    modules = mass_import(
        get_module_names('ziutek', 'ziutek', '*.py', DOCTEST_BLACKLIST)
    )
    for mod in modules:
        suite.addTest(doctest.DocTestSuite(mod))
    suite.addTest(doctest.DocFileSuite('../../index.rst'))
    return suite


def all_tests_suite():
    suite = unittest.TestLoader().loadTestsFromNames(
        get_module_names('ziutek.tests', 'ziutek/tests', 'test_*.py', [])
    )
    suite = additional_tests(suite)
    return unittest.TestSuite([suite])


def main():
    runner = unittest.TextTestRunner()
    suite = all_tests_suite()
    runner.run(suite)


if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    main()
