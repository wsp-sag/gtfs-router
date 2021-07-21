# gtfs-router
Collection of GTFS Pathbuilding Functions

# DISCLAIMER
This project is still a work in progress. The code has been tested on several GTFS feeds, but WSP USA makes no claims or warrants about the results of the algorithms contained in this repository. USE AT YOUR OWN RISK!

# Install
`pip install .`

or and [editable install](https://packaging.python.org/guides/distributing-packages-using-setuptools/#id68)  
`pip install -e .`

# Examples
The [examples](examples)  folder contains a sample setup to demonstrate how to use the router along with information about reporting results.

## Raptor
The RAPTOR pathbuilder is based on [Round-Based Public Transit Routing](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/raptor_alenex.pdf) algorithm developed by Daniel Delling, Thomas Pajor, and Renato F. Werneck at Microsoft Research Silicon Valley.

The RAPTOR algorithm also builds off of a [Gist](https://gist.github.com/kuanb/a45b65c3135dce717497643e7f35f0ab) and a series of [blog posts from Kuan Butts](http://kuanbutts.com/2020/09/14/raptor-with-cache/). The algorithm contained in this repository relies on more vectorization to improve performance, but Kuan's prototypes were instrumental in understanding the algorithm publised in the research paper.


## WSP Point of Contact
The WSP points of contact for this software is Clint Daniels (@danielsclint).
