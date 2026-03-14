<%
    all_cols = dim_keys + write_only_cols + ["value"]
    all_params = ["p_" + k for k in dim_keys] + ["p_" + k for k in write_only_cols] + ["p_value"]
%>BEGIN
    INSERT INTO ${api_view} (${', '.join(all_cols)})
    VALUES (${', '.join(all_params)});
END;
