<%
    insert_cols = ", ".join(all_cols)
    entry_insert_cols = "entry_id, " + insert_cols
%>\
% if diff_mode:
<%
    group_cols = ", ".join(diff_keys)
    select_cols = ", ".join(
        [f"combined.{k}" for k in diff_keys]
        + ["SUM(combined.value) AS value"]
        + [f"MAX(combined.{c}) AS {c}" for c in passthrough_cols]
    )
%>\
WITH
  _eid AS (SELECT gen_random_uuid() AS eid),
  input AS (${input_sql}),
  desired AS (${desired_sql}),
  existing AS (${existing_sql}),
  deltas AS (
    SELECT ${select_cols}
    FROM (
      SELECT ${desired_cols} FROM desired
      UNION ALL
      SELECT ${existing_cols} FROM existing
    ) combined
    GROUP BY ${group_cols}
    HAVING SUM(combined.value) != 0
  )
INSERT INTO ${api_view} (${entry_insert_cols})
SELECT (SELECT eid FROM _eid), ${insert_cols} FROM deltas
RETURNING *
% else:
WITH
  _eid AS (SELECT gen_random_uuid() AS eid),
  input AS (${input_sql})
INSERT INTO ${api_view} (${entry_insert_cols})
SELECT (SELECT eid FROM _eid), ${insert_cols} FROM input
RETURNING *
% endif
