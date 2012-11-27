.. junction documentation master file, created by
   sphinx-quickstart on Tue Nov 16 18:31:29 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to junction's documentation!
====================================

Junction is a publish/subscribe and RPC system for python, geared
towards distributing work on an internal network.

It uses greenhouse_ for the parallel IO across peers and for blocking
syncronous calls, and mummy_ for fast data serialization.


API Reference
-------------

.. toctree::

   getting_started
   programming_with_futures
   junction/hub
   junction/client
   junction/futures
   junction/errors
   junction/hooks

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _greenhouse: http://teepark.github.com/greenhouse
.. _mummy: http://github.com/teepark/mummy
