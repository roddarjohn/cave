<%
    dk_array = "ARRAY[" + ", ".join("'" + k + "'" for k in diff_keys) + "]"
    wk_array = (
        "ARRAY[" + ", ".join("'" + k + "'" for k in write_only_cols) + "]::TEXT[]"
        if write_only_cols else "ARRAY[]::TEXT[]"
    )
    wp_array = (
        "ARRAY[" + ", ".join("p_" + k + "::TEXT" for k in write_only_cols) + "]::TEXT[]"
        if write_only_cols else "ARRAY[]::TEXT[]"
    )
    insert_cols = ", ".join(diff_keys + write_only_cols + ["value"])
    select_diff = ", ".join("t." + k for k in diff_keys)
    select_wo = (", ".join("p_" + k for k in write_only_cols) + ", ") if write_only_cols else ""
%>DECLARE
    v_delta_count BIGINT;
BEGIN
    SELECT delta_count INTO v_delta_count
    FROM ${utility_schema}.ledger_apply_state(
        '${table}',
        '${api_view}',
        '${staging_table}',
        ${dk_array},
        ${wk_array},
        ${wp_array}
    );
% if not partial:
    INSERT INTO ${api_view} (${insert_cols})
    SELECT ${select_diff}, ${select_wo}-SUM(t.value)
    FROM ${table} t
    WHERE (${', '.join('t.' + k for k in diff_keys)}) NOT IN (
        SELECT ${', '.join(diff_keys)} FROM ${staging_table}
    )
    GROUP BY ${select_diff}
    HAVING SUM(t.value) <> 0;
% endif
    TRUNCATE ${staging_table};
    delta := v_delta_count;
    RETURN NEXT;
    RETURN;
END;
