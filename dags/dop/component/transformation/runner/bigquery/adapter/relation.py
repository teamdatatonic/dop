from dop.component.transformation.common.adapter.relation import (
    BaseRelation,
    RelationValueError,
)

from google.cloud import bigquery
from dataclasses import dataclass

from dop.component.transformation.runner.bigquery.adapter.model import PartitionConfig


@dataclass(frozen=True)
class BigQueryRelation(BaseRelation):
    database: str
    schema: str
    identifier: str

    def __post_init__(self):
        if (
            not self.database
            or not self.schema
            or not self.identifier
            or any(
                [len(self.database) < 1, len(self.schema) < 1, len(self.identifier) < 1]
            )
        ):
            raise RelationValueError(
                f"database: `{self.database}`, schema: `{self.schema}` "
                f"and identifier: `{self.identifier}` must not be empty"
            )

    def __repr__(self):
        return f"`{self.database}.{self.schema}.{self.identifier}`"


class RelationHelper:
    def __init__(self, client: bigquery.client.Client):
        self._client = client

    def check_relation_exists(self, relation: BaseRelation):
        query = f"""
        SELECT * FROM {relation.database}.{relation.schema}.INFORMATION_SCHEMA.TABLES
        WHERE table_name = '{relation.identifier}';
"""

        for _ in self._client.query(query=query):
            return True

        return False

    def has_same_partition_definition(
        self, partition_config: PartitionConfig, relation: BaseRelation
    ):
        existing_partition_definition = self.partition_definition(relation=relation)

        if not partition_config or not existing_partition_definition:
            return True
        elif (
            partition_config.field == existing_partition_definition["column_name"]
            and partition_config.data_type == existing_partition_definition["data_type"]
        ):
            return True
        else:
            return False

    def partition_definition(self, relation: BaseRelation):
        query = f"""
        SELECT column_name, data_type FROM {relation.database}.{relation.schema}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{relation.identifier}' AND is_partitioning_column='YES' LIMIT 1;
"""

        for row in self._client.query(query=query):
            return {"column_name": row["column_name"], "data_type": row["data_type"]}

        return None

    def check_if_schemas_match(
        self, tmp_relation: BaseRelation, relation: BaseRelation
    ):
        query_template = """
                SELECT CONCAT(coalesce(column_name,''),coalesce(is_nullable,''),coalesce(data_type,''), coalesce(is_partitioning_column, '')) as col_schema_idendifer
                FROM {r.database}.{r.schema}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{r.identifier}' ORDER BY ordinal_position;
"""
        tmp_schema = [
            x["col_schema_idendifer"]
            for x in self._client.query(query=query_template.format(r=tmp_relation))
        ]
        target_schema = [
            x["col_schema_idendifer"]
            for x in self._client.query(query=query_template.format(r=relation))
        ]

        if tmp_schema == target_schema:
            return True
        return False

    def get_columns_of_relation(self, relation: BaseRelation):
        query = f"""
                SELECT column_name FROM {relation.database}.{relation.schema}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{relation.identifier}' ORDER BY ordinal_position;
"""

        return [x["column_name"] for x in self._client.query(query=query)]
