import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='ethToSql',
      version='0.3',
      description='Read Data from an Ethereum node and insert into a SQL database',
      long_description=long_description,
      url='https://github.com/dmenin/EthToSql',
      author='Diego',
      author_email='dmenin@gmail.com',
      license='MIT',
      packages=setuptools.find_packages(),
      install_requires=[
          'tqdm',
      ],
      zip_safe=False,
	  test_suite='nose.collector',
	  tests_require=['nose'],
      classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
      )
)