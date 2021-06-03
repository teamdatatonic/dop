def implode_arguments(dbt_arguments, filter_func=None):
    filtered_dbt_arguments = (
        filter_func(dbt_arguments) if filter_func else dbt_arguments
    )
    return " ".join(
        [
            " ".join(
                [
                    argument["option"],
                    "" if argument.get("value") is None else argument.get("value"),
                ]
            )
            for argument in filtered_dbt_arguments
        ]
    )


def parsed_cmd_airflow_context_vars(context):
    cmd = '"{'
    context_vars = ["ds", "ds_nodash", "ts", "ts_nodash", "ts_nodash_with_tz"]
    if context:
        var_list = [f"'{v}'" + f": '{context[v]}'" for v in context_vars]
    else:
        var_list = [f"'{v}'" + ": '{{ " + v + " }}'" for v in context_vars]
    cmd += ",".join(var_list)

    cmd += '}"'

    return cmd
