<%
    insert_cols = ", ".join(all_cols)
%>\
% if diff_mode:
WITH
  input AS (${input_sql}),
  desired AS (${desired_sql}),
  existing AS (${existing_sql}),
  deltas AS (
    SELECT ${insert_cols}
    FROM (
      SELECT ${desired_cols} FROM desired
      UNION ALL
      SELECT ${existing_cols} FROM existing
    ) combined
    WHERE value != 0
  )
INSERT INTO ${api_view} (${insert_cols})
SELECT ${insert_cols} FROM deltas
RETURNING *
% else:
WITH input AS (${input_sql})
INSERT INTO ${api_view} (${insert_cols})
SELECT ${insert_cols} FROM input
RETURNING *
% endif
