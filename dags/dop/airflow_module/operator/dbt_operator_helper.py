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


def extract_argument(dbt_arguments: list, name: str, default_value: str = None):
    """
    Extract an argument from the argument list. Format is
    [
        {'option': 'OPTION1', 'value': 'VALUE1'},
        {'option': 'OPTION2', 'value': 'VALUE2'},
        ...
    ]

    :param dbt_arguments: Argument list
    :param name: Argument to extract
    :param default_value: Default value to return if not present
    """
    return next(
        (arg.get("value") for arg in dbt_arguments if arg.get("option") == name),
        default_value,
    )
