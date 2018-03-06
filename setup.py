from setuptools import setup, find_packages
 
setup(name='migbq',
      version='0.0.81',
      url='https://github.com/jo8937/migbq',
      license='MIT',
      author='jo8937',
      author_email='jo8937@gmail.com',
      description='read microsoft sql server table data and upload to bigquery',
      packages=find_packages(include=["migbq"],exclude=['tests','tools']),
      long_description=open('README.md').read(),
      zip_safe=False,
      python_requires=">=2.7",
      install_requires=[
        'ujson>=1.35',
        'peewee==2.8.5',
        'pymssql>=2.1.1',
        'peewee-mssql>=0.1.0',
        'concurrent-log-handler>=0.9.8',
        'pyyaml>=3.12',
        'google-cloud-bigquery==0.27.0',
        'Jinja2>=2.10',
        'httplib2'
        ],
      entry_points={
            'console_scripts': [
                'migbq = migbq.BQMig:commander',
            ],
        }
      )