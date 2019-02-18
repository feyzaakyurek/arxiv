from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='arxiv',
      version='0.1',
      description='Collaboration distance on Arxiv',
      url='https://github.com/feyzaakyurek/arxiv',
      author='Afra Feyza Akyurek',
      author_email='feyza@cmu.edu',
      license='MIT',
      packages=['arxiv'],
      zip_safe=False)
