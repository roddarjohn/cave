Benchmarks
==========

pgcraft benchmarks exercise the trigger-based API views for each
dimension type against a real PostgreSQL instance.  Run them with
``just bench`` (see :doc:`development` for setup and options).

What is benchmarked
-------------------

**Simple dimension** — single-row INSERT, UPDATE, DELETE via API view
triggers; batch INSERT (100 and 1000 rows); SELECT and filtered SELECT
on 10k-row tables.

**Append-only dimension** — single-row INSERT and UPDATE (which appends
a new attribute row); batch INSERT (100 rows); SELECT from the join view
on 10k entities; SELECT after 100 revisions of a single entity.

**EAV dimension** — single-row INSERT and single-attribute UPDATE via
the pivot view; batch INSERT (100 rows); full pivot view SELECT and
filtered pivot SELECT on 10k entities.

**Ledger** — single-row INSERT; batch INSERT (100 and 1000 rows);
balance aggregation query on 10k entries.

Representative results
----------------------

The tables below were captured on a local development machine
(PostgreSQL 16, Linux, NVMe SSD).  All times are in **microseconds
(us)**.  Your numbers will vary — run ``just bench`` to measure on
your own hardware.

Simple dimension
~~~~~~~~~~~~~~~~

A single backing table with an API view wired up via INSTEAD OF
triggers for INSERT, UPDATE, and DELETE.

.. list-table::
   :header-rows: 1
   :widths: 35 30 12 12 12 9

   * - Benchmark
     - Description
     - Min
     - Mean
     - Median
     - Rounds
   * - ``test_insert_single``
     - Insert one row through the API view trigger.
     - 571.78
     - 747.73
     - 718.17
     - 10,000
   * - ``test_update_single``
     - Update one row through the API view trigger.
     - 569.78
     - 707.57
     - 684.11
     - 10,000
   * - ``test_delete_single``
     - Delete one row through the API view trigger (includes
       re-inserting the row each round).
     - 1,566.23
     - 1,879.28
     - 1,830.61
     - 10,000
   * - ``test_insert_batch_100``
     - Insert 100 rows in a single statement via the API view.
     - 1,262.88
     - 1,427.57
     - 1,408.23
     - 100
   * - ``test_insert_batch_1000``
     - Insert 1,000 rows in a single statement via the API view.
     - 8,033.82
     - 8,765.88
     - 8,701.54
     - 100
   * - ``test_select_all``
     - ``SELECT *`` from the API view over a 10k-row table.
     - 3,644.55
     - 6,101.82
     - 4,080.80
     - 1,000
   * - ``test_select_filtered``
     - ``SELECT * WHERE name = ...`` on a 10k-row table.
     - 644.99
     - 706.25
     - 686.46
     - 1,000

Append-only dimension
~~~~~~~~~~~~~~~~~~~~~

Two tables (root + attributes) joined through a view.  Updates append
a new attribute row and re-point the root's foreign key, preserving
full history.

.. list-table::
   :header-rows: 1
   :widths: 35 30 12 12 12 9

   * - Benchmark
     - Description
     - Min
     - Mean
     - Median
     - Rounds
   * - ``test_insert_single``
     - Insert one entity (writes to both root and attributes tables).
     - 525.82
     - 743.17
     - 663.42
     - 10,000
   * - ``test_update_single``
     - Update one entity (appends a new attribute row and updates
       the root foreign key).
     - 640.70
     - 789.45
     - 756.41
     - 10,000
   * - ``test_insert_batch_100``
     - Insert 100 entities in a single statement.
     - 1,583.00
     - 1,799.71
     - 1,756.50
     - 100
   * - ``test_select_latest``
     - ``SELECT *`` from the join view over 10k entities.
     - 12,311.89
     - 15,407.37
     - 13,557.13
     - 1,000
   * - ``test_select_after_many_updates``
     - ``SELECT *`` from the join view for an entity that has
       accumulated 100 historical revisions.
     - 103.97
     - 118.79
     - 108.61
     - 1,000

EAV dimension
~~~~~~~~~~~~~

Entity-Attribute-Value storage with a pivot view that presents
attributes as columns.  Each INSERT fans out into one entity row
plus one attribute row per column.

.. list-table::
   :header-rows: 1
   :widths: 35 30 12 12 12 9

   * - Benchmark
     - Description
     - Min
     - Mean
     - Median
     - Rounds
   * - ``test_insert_single``
     - Insert one entity with two attributes through the pivot view.
     - 541.10
     - 727.88
     - 692.36
     - 10,000
   * - ``test_update_single_attribute``
     - Update one attribute on an existing entity (appends a new
       attribute row).
     - 4,034.38
     - 11,412.53
     - 11,342.63
     - 10,000
   * - ``test_insert_batch_100``
     - Insert 100 entities in a single statement through the pivot
       view.
     - 3,551.98
     - 3,768.82
     - 3,700.29
     - 100
   * - ``test_select_pivot``
     - ``SELECT *`` from the pivot view over 10k entities.
     - 21,552.03
     - 25,030.05
     - 23,276.44
     - 1,000
   * - ``test_select_pivot_filtered``
     - ``SELECT * WHERE sku = ...`` from the pivot view over 10k
       entities.
     - 11,122.92
     - 12,603.28
     - 12,220.87
     - 1,000

Ledger
~~~~~~

Append-only, immutable ledger table.  Only INSERT is allowed;
UPDATE and DELETE are blocked by triggers.

.. list-table::
   :header-rows: 1
   :widths: 35 30 12 12 12 9

   * - Benchmark
     - Description
     - Min
     - Mean
     - Median
     - Rounds
   * - ``test_insert_single``
     - Insert one ledger entry through the API view.
     - 565.25
     - 713.93
     - 688.73
     - 10,000
   * - ``test_insert_batch_100``
     - Insert 100 ledger entries in a single statement.
     - 2,322.23
     - 2,640.78
     - 2,523.16
     - 100
   * - ``test_insert_batch_1000``
     - Insert 1,000 ledger entries in a single statement.
     - 11,736.55
     - 12,518.27
     - 12,348.01
     - 100
   * - ``test_select_balance``
     - ``SELECT *`` from a balance aggregation view (``SUM(value)
       GROUP BY category``) over 10k entries.
     - 1,563.08
     - 1,679.10
     - 1,636.57
     - 1,000
