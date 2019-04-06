=================
SQLAlchemy Dremio
=================


.. image:: https://img.shields.io/pypi/v/sqlalchemy_dremio.svg
        :target: https://pypi.python.org/pypi/sqlalchemy_dremio


An SQLAlchemy dialect for Dremio which simply wraps the PyODBC Dremio interface.


* Free software: MIT license
* Documentation: https://sqlalchemy-dremio.readthedocs.io.

Installation
------------

`pip install sqlalchemy_dremio`

Features
--------

* All basic sqlalchmey functions will work
* Works with superset

Superset integration
--------------------

Install superset
----------------

`pip3 install superset` # install superset
`pip3 show superset` # Check where its installed
`subl /usr/local/lib/python3.7/site-packages/superset` #open the folder with your fav editor

Make the following changes
--------------------------

dataframe.py
------------

From

```
        data = data or []
        self.df = (
                    pd.DataFrame(list(data), columns=self.column_names).infer_objects())
```

to

```
        data = data or []

        colcount = len(column_names)
        rowcount = len(data)
        data = np.reshape(data, (rowcount, colcount))

        self.df = (
            pd.DataFrame(list(data), columns=self.column_names).infer_objects())

```

sql_lab.py
----------

Add the following next to `import sqlalchemy`

`import sqlalchemy_dremio.pyodbc`


Start superset by following the instructions here: https://superset.incubator.apache.org/installation.html#superset-installation-and-initialization

Use the following syntax to add dremio as a source:

dremio+pyodbc://<username>:<password>@<host>:31010/dremio


Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.
https://github.com/uhjish

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

