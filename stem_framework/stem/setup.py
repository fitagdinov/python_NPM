from setuptools import setup, find_packages

setup(
   name='stem',
   version='1.0',
   description='A useful module',
   author='Robert',
   author_email='robertfitagdinov2gmail.com',
   packages=['stem'],  #same as name
   install_requires=['numpy', 'bar', 'greek'], #external packages as dependencies
)
