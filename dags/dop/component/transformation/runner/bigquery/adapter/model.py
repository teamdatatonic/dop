from dataclasses import dataclass
from typing import Optional, Dict, Any

from dop.component.transformation.common.adapter.model import Argument


@dataclass
class PartitionConfig:
    field: str
    data_type: str = 'date'
    range: Optional[Dict[str, Any]] = None

    def render(self, alias: Optional[str] = None):
        column: str = self.field
        if alias:
            column = f'{alias}.{self.field}'

        if self.data_type in ('timestamp', 'datetime'):
            return f'date({column})'
        else:
            return column

    @staticmethod
    def create(options):
        if not options.get('partition_key') or not options.get('partition_data_type'):
            return None
        elif options.get('partition_data_type') == 'int64':
            if not options.get('partition_key') \
                    or not options.get('partition_range') \
                    or not options.get('partition_range').get('start') \
                    or not options.get('partition_range').get('end') \
                    or not options.get('partition_range').get('interval'):
                raise RuntimeError(f'Invalid partition options: {options}')
        elif options.get('partition_data_type') in ['date', 'timestamp', 'datetime']:
            if not options.get('partition_key'):
                raise RuntimeError(f'Invalid partition options: {options}')
        else:
            raise NotImplementedError(f'Partition data type: {options.get("partition_data_type")} is not supported')

        return PartitionConfig(
            field=options.get('partition_key'),
            data_type=options.get('partition_data_type'),
            range=options.get('partition_range')
        )


@dataclass
class TableOptionsConfig:
    options: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class UDFArgument(Argument):
    pass


@dataclass(frozen=True)
class StoredProcedureArgument(Argument):
    pass
