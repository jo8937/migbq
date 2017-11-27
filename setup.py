from setuptools import setup, find_packages
 
setup(name='migbq',
      version='0.0.1',
      url='https://github.com/jo8937/rdbms-to-bigquery-data-loader',
      license='MIT',
      author='jo8937',
      author_email='jo8937@gmail.com',
      description='read rdbms table data and upload to bigquery',
      packages=find_packages(exclude=['tests']),
      long_description=open('README.md').read(),
      zip_safe=False,
      setup_requires=[
        'google-cloud-bigquery==0.27.0',
        'ujson',
        'peewee',
        'peewee-mssql',
        'pymssql',
        'concurrent-log-handler'
        ],
      entry_points={
            'console_scripts': [
                'migbq = migbq.BQMig:commander',
            ],
        },
      )