import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name,
    check_relation_types,
    check_relations_equal,
)

from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
)


class TestSimpleMaterializationsFlink(BaseSimpleMaterializations):
    pass


class TestSingularTestsFlink(BaseSingularTests):
    pass
#
#
# class TestSingularTestsEphemeralFlink(BaseSingularTestsEphemeral):
#     pass
#
#
class TestEmptyFlink(BaseEmpty):
    pass


# class TestEphemeralFlink(BaseEphemeral):
#     pass
#
#
# class TestIncrementalFlink(BaseIncremental):
#     pass
#
#
class TestGenericTestsFlink(BaseGenericTests):
    pass


# class TestSnapshotCheckColsFlink(BaseSnapshotCheckCols):
#     pass
#

# class TestSnapshotTimestampFlink(BaseSnapshotTimestamp):
#     pass
#
#
# class TestBaseAdapterMethodFlink(BaseAdapterMethod):
#     pass
