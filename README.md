
Overview
========

An attempt to understand the geocoder built into the OpenBlock software well enough that it can be pulled out as a standalone project.  

<http://developer.openblockproject.org/>

Right now, the geocoder depends on having a Postgis-enabled Postgres database running on the same machine, with the 'blocks' and 'intersections' tables included, as set up by the Openblock installation process.

Ultimately, we want the code to be able to run independent of any Openblock installation, or possibly even of Postgis itself (through dependence on a freely-availably Python library like GDAL).  

This is all shamelessly ripped off of the public Everyblock code (in particular, the 'ebpub' application inside OpenBlock).  

A project started during the Boston OpenBlock hackday: 

<http://blog.openblockproject.org/post/1300839360/come-hack-on-openblock-in-boston-on-oct-30th>

Code 
====

    .
    |-- README.md
    `-- djeocoder
        |-- __init__.py
        |-- djeocoder.py
        |-- postgis.py
        `-- test.py
        `-- parser
            |-- README.md
            |-- __init__.py
            |-- abbr_state.txt
            |-- cities.py
            |-- make_cf_tests.py
            |-- numbered_streets.py
            |-- parsing.py
            |-- states.py
            |-- suffixes.py
            |-- suffixes.txt
            `-- tests.py
