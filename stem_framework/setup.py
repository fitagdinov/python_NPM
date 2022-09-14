from setuptools import setup, find_packages
from sphinx.setup_command import BuildDoc

name='stem',
version='1.0',
cmdclass = {'build_sphinx': BuildDoc}
setup(
    # name=name,
    # version=version,
    # description='A useful module',
    # author='Robert',
    # author_email='robertfitagdinov@gmail.com',
    # cmdclass=cmdclass,
    # packages=find_packages(),  #same as name
    # install_requires=['numpy', 'bar', 'greek'], #external packages as dependencies
    # command_options={
    #     'build_sphinx': {
    #         'project': ('setup.py', name),
    #         'version': ('setup.py', version),
    #         'source_dir': ('setup.py', 'docs')}}
)
