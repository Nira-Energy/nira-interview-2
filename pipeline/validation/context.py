"""Great Expectations DataContext management.

Uses the in-memory/ephemeral DataContext pattern for pipeline validation
without requiring a full GE project directory.
"""

from pathlib import Path

import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
)

type ContextConfig = dict[str, str | bool]

# Default configuration for the pipeline's GE context
_DEFAULT_DATASOURCE_CONFIG = DatasourceConfig(
    class_name="PandasDatasource",
    module_name="great_expectations.datasource",
)

_CONTEXT_ROOT = Path(__file__).parent / "ge_config"


def get_data_context(
    project_root: Path | None = None,
    ephemeral: bool = True,
) -> BaseDataContext:
    """Initialize and return a Great Expectations DataContext.

    By default uses an ephemeral (in-memory) context suitable for CI/CD
    pipelines where we don't need to persist validation results to a store.
    """
    if not ephemeral and project_root:
        return ge.data_context.DataContext(context_root_dir=str(project_root))

    config = DataContextConfig(
        datasources={
            "pipeline_datasource": _DEFAULT_DATASOURCE_CONFIG,
        },
        store_backend_defaults=InMemoryStoreBackendDefaults(),
    )
    context = BaseDataContext(project_config=config)
    return context


def get_datasource_from_context(context: BaseDataContext) -> str:
    """Return the name of the default pipeline datasource."""
    datasources = list(context.datasources.keys())
    if not datasources:
        raise RuntimeError("No datasources configured in GE context")
    return datasources[0]
