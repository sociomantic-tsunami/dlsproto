Description
===========

``dlsproto`` is a library that contains the protocol for the Distributed Log
Store (DLS), including:

* The DLS client (``src.dlsproto.client``).
* Base classes for the protocol handling parts of the DLS node
  (``src.dlsproto.node``).
* A simple, "fake" DLS node, for use in tests (``src.fakedls``).
* A turtle env extension (``src.turtle.env.Dls``) providing a fake DLS node
  for use in tests, including methods to inspect and modify its contents.
* A thorough test of the DLS protocol, using the client to connect to a node.
  The test is run, in this repo, on a fake node, but it can be reused in other
  repos to test real node implementations. (``src.dlstest``)

Dependencies
============

==========  =======
Dependency  Version
==========  =======
ocean       v4.0.x
swarm       v5.0.x
turtle      v9.0.x
makd        v2.1.3
==========  =======

Versioning
==========

dlsproto's versioning follows `Neptune
<https://github.com/sociomantic-tsunami/neptune/blob/master/doc/library-user.rst>`_.

This means that the major version is increased for breaking changes, the minor
version is increased for feature releases, and the patch version is increased
for bug fixes that don't cause breaking changes.

Support Guarantees
------------------

* Major branch development period: 6 months
* Maintained minor versions: 1 most recent

Maintained Major Branches
-------------------------

======= ==================== =============== =====
Major   Initial release date Supported until Notes
======= ==================== =============== =====
v13.x.x v13.0.0_: 03/08/2017 TBD             First open source release
======= ==================== =============== =====

.. _v13.0.0: https://github.com/sociomantic-tsunami/dlsproto/releases/tag/v13.0.0
