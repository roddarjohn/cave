Benchmarks
==========

pgcraft benchmarks measure the performance of each dimension type's
backing tables and views against a real PostgreSQL instance.  Run them
with ``just bench`` (see :doc:`development` for setup and CLI options).

What is benchmarked
-------------------

Each dimension type is exercised with a consistent set of operations:

**Simple dimension** — a single backing table with a read-only view.
Benchmarks cover single-row INSERT, UPDATE, and DELETE on the table,
batch INSERTs (100 and 1,000 rows), and SELECT queries on the view
over a 10k-row dataset.

**Append-only dimension** — a root table joined to an attributes table
through a view.  Updates append a new attribute row and re-point the
root foreign key, preserving full history.  Benchmarks cover single-row
INSERT and UPDATE, batch INSERT, SELECT from the join view, and SELECT
after an entity has accumulated 100 historical revisions.

**EAV dimension** — entity-attribute-value storage with a pivot view
that presents attributes as columns.  Benchmarks cover single-row
INSERT (fans out to one entity row plus one attribute row per column),
single-attribute UPDATE, batch INSERT, and SELECT from the pivot view.

**Ledger** — an append-only, immutable table.  Benchmarks cover
single-row INSERT, batch INSERTs (100 and 1,000 rows), and a balance
aggregation query (``SUM(value) GROUP BY category``) over 10k entries.

Representative results
----------------------

The tables below were generated with ``just bench-docs`` on a local
development machine (PostgreSQL 16, Linux, NVMe SSD).  Your numbers
will vary — run ``just bench`` to measure on your own hardware.

.. include:: benchmark_results.rst
