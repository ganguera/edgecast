from setuptools import setup

setup(
  name='edgecast',
  version='0.1',
  description='EdgeCast Usage Report',
  license='MIT',
  packages=['edgecast'],
  install_requires=[
    'arrow',
    'requests'
  ],
  entry_points = {
    'console_scripts': ['edgecast=edgecast.command_line:main']
  },
  zip_safe=False
)
