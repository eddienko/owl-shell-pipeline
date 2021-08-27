Owl Shell Pipeline
==================

This is an `Owl Pipeline <https://eddienko.github.io/owl-pipeline>`__ that runs
a script in the cluster.


Install
-------

.. code-block:: bash

  curl -O https://raw.githubusercontent.com/eddienko/owl-shell-pipeline/main/shell_pipeline/signature.yaml
  owl admin pdef add signature.yaml


Pipeline Definition
-------------------

An example pipeline definition file is:

  .. code-block:: yaml

    # Version of the configuration file
    version: 1

    # Name of the pipeline
    name: shell

    command: |
      echo "Hello"
      sleep 300

    resources:
      cores: 1
      workers: 1
      memory: 1
