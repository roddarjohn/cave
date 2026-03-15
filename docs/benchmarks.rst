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

The table below was captured on a local development machine
(PostgreSQL 16, Linux, NVMe SSD).  Your numbers will vary — use
``just bench`` to measure on your own hardware.

Simple dimension
~~~~~~~~~~~~~~~~

.. code-block:: text

   Name (time in us)          Min          Mean        Median     Rounds
   -----------------------------------------------------------------------
   test_insert_single       571.78       747.73       718.17     10,000
   test_update_single       569.78       707.57       684.11     10,000
   test_delete_single     1,566.23     1,879.28     1,830.61    10,000
   test_insert_batch_100  1,263         1,428        1,408         100
   test_insert_batch_1000 8,034         8,766        8,702         100
   test_select_all        3,645         6,102        4,081       1,000
   test_select_filtered     644.99       706.25       686.46     1,000

Append-only dimension
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

   Name (time in us)                     Min          Mean        Median     Rounds
   ----------------------------------------------------------------------------------
   test_insert_single                  525.82       743.17       663.42     10,000
   test_update_single                  640.70       789.45       756.41     10,000
   test_insert_batch_100             1,583         1,800        1,757         100
   test_select_latest               12,312        15,407       13,557       1,000
   test_select_after_many_updates     103.97       118.79       108.61      1,000

EAV dimension
~~~~~~~~~~~~~

.. code-block:: text

   Name (time in us)               Min          Mean        Median     Rounds
   ----------------------------------------------------------------------------
   test_insert_single             541.10       727.88       692.36     10,000
   test_update_single_attribute 4,034        11,413       11,343      10,000
   test_insert_batch_100        3,552         3,769        3,700         100
   test_select_pivot           21,552        25,030       23,276       1,000
   test_select_pivot_filtered  11,123        12,603       12,221       1,000

Ledger
~~~~~~

.. code-block:: text

   Name (time in us)          Min          Mean        Median     Rounds
   -----------------------------------------------------------------------
   test_insert_single       565.25       713.93       688.73     10,000
   test_insert_batch_100  2,322         2,641        2,523         100
   test_insert_batch_1000 11,737       12,518       12,348         100
   test_select_balance    1,563         1,679        1,637       1,000
