import os
import sys
import time
import datetime
import logging

from itertools import chain
from itertools import product
from functools import reduce
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np
import pytz

from cloudvolume import CloudVolume
from multiwrapper import multiprocessing_utils as mu
from google.api_core.retry import Retry, if_exception_type
from google.api_core.exceptions import Aborted, DeadlineExceeded, ServiceUnavailable
from google.auth import credentials
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import (
    TimestampRange,
    TimestampRangeFilter,
    ColumnRangeFilter,
    ValueRangeFilter,
    RowFilterChain,
    ColumnQualifierRegexFilter,
    ConditionalRowFilter,
    PassAllFilter,
    RowFilter,
)
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.column_family import MaxVersionsGCRule

from . import (
    exceptions as cg_exceptions,
    edits as cg_edits,
    cutting,
    misc,
)
from .meta import ChunkedGraphMeta
from .utils.generic import (
    compute_indices_pandas,
    compute_bitmasks,
    get_valid_timestamp,
    get_time_range_filter,
    get_time_range_and_column_filter,
    get_max_time,
    combine_cross_chunk_edge_dicts,
    get_min_time,
    partial_row_data_to_column_dict,
)
from .utils import serializers, column_keys, row_keys, basetypes
from .operation import (
    GraphEditOperation,
    MergeOperation,
    MulticutOperation,
    SplitOperation,
    RedoOperation,
    UndoOperation,
)
from .edges import Edges
from .edges.utils import concatenate_chunk_edges
from .edges.utils import filter_edges
from .edges.utils import get_active_edges
from .chunks.utils import compute_chunk_id
from ..io.edges import get_chunk_edges


HOME = os.path.expanduser("~")
N_DIGITS_UINT64 = len(str(np.iinfo(np.uint64).max))
N_BITS_PER_ROOT_COUNTER = np.uint64(8)
LOCK_EXPIRED_TIME_DELTA = datetime.timedelta(minutes=3, seconds=0)

# Setting environment wide credential path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    HOME + "/.cloudvolume/secrets/google-secret.json"
)


class ChunkedGraph(object):
    def __init__(
        self,
        table_id: str,
        project_id: str = "neuromancer-seung-import",
        instance_id: str = "pychunkedgraph",
        chunk_size: Tuple[np.uint64, np.uint64, np.uint64] = None,
        fan_out: Optional[np.uint64] = None,
        use_skip_connections: Optional[bool] = True,
        edge_dir: Optional[str] = None,
        s_bits_atomic_layer: Optional[np.uint64] = 8,
        n_bits_root_counter: Optional[np.uint64] = 0,
        n_layers: Optional[np.uint64] = None,
        credentials: Optional[credentials.Credentials] = None,
        client: bigtable.Client = None,
        dataset_info: Optional[object] = None,
        is_new: bool = False,
        logger: Optional[logging.Logger] = None,
        meta: Optional[ChunkedGraphMeta] = None,
    ) -> None:

        if logger is None:
            self.logger = logging.getLogger(f"{project_id}/{instance_id}/{table_id}")
            self.logger.setLevel(logging.WARNING)
            if not self.logger.handlers:
                sh = logging.StreamHandler(sys.stdout)
                sh.setLevel(logging.WARNING)
                self.logger.addHandler(sh)
        else:
            self.logger = logger

        if client is not None:
            self._client = client
        else:
            self._client = bigtable.Client(
                project=project_id, admin=True, credentials=credentials
            )

        self._instance = self.client.instance(instance_id)
        self._table_id = table_id

        self._table = self.instance.table(self.table_id)

        if is_new:
            self._check_and_create_table()

        self._dataset_info = self.check_and_write_table_parameters(
            column_keys.GraphSettings.DatasetInfo,
            dataset_info,
            required=True,
            is_new=is_new,
        )

        self._cv_path = self._dataset_info["data_dir"]  # required
        self._mesh_dir = self._dataset_info.get("mesh", None)  # optional
        self._edge_dir = self.check_and_write_table_parameters(
            column_keys.GraphSettings.EdgeDir, edge_dir, required=False, is_new=is_new
        )

        self._n_layers = self.check_and_write_table_parameters(
            column_keys.GraphSettings.LayerCount, n_layers, required=True, is_new=is_new
        )
        self._fan_out = self.check_and_write_table_parameters(
            column_keys.GraphSettings.FanOut, fan_out, required=True, is_new=is_new
        )
        s_bits_atomic_layer = self.check_and_write_table_parameters(
            column_keys.GraphSettings.SpatialBits,
            np.uint64(s_bits_atomic_layer),
            required=False,
            is_new=is_new,
        )
        self._use_skip_connections = (
            self.check_and_write_table_parameters(
                column_keys.GraphSettings.SkipConnections,
                np.uint64(use_skip_connections),
                required=False,
                is_new=is_new,
            )
            > 0
        )
        self._n_bits_root_counter = self.check_and_write_table_parameters(
            column_keys.GraphSettings.RootCounterBits,
            np.uint64(n_bits_root_counter),
            required=False,
            is_new=is_new,
        )
        self._chunk_size = self.check_and_write_table_parameters(
            column_keys.GraphSettings.ChunkSize,
            chunk_size,
            required=True,
            is_new=is_new,
        )

        self._dataset_info["graph"] = {"chunk_size": self.chunk_size}

        self._bitmasks = compute_bitmasks(
            self.n_layers, self.fan_out, s_bits_atomic_layer
        )

        self._cv = None

        # Hardcoded parameters
        self._n_bits_for_layer_id = 8
        self._cv_mip = 0

        # Vectorized calls
        self._get_chunk_layer_vec = np.vectorize(self.get_chunk_layer)
        self._get_chunk_id_vec = np.vectorize(self.get_chunk_id)

        self.meta = meta

    @property
    def client(self) -> bigtable.Client:
        return self._client

    @property
    def instance(self) -> bigtable.instance.Instance:
        return self._instance

    @property
    def table(self) -> bigtable.table.Table:
        return self._table

    @property
    def table_id(self) -> str:
        return self._table_id

    @property
    def instance_id(self):
        return self.instance.instance_id

    @property
    def project_id(self):
        return self.client.project

    @property
    def family_id(self) -> str:
        return "0"

    @property
    def incrementer_family_id(self) -> str:
        return "1"

    @property
    def log_family_id(self) -> str:
        return "2"

    @property
    def cross_edge_family_id(self) -> str:
        return "3"

    @property
    def family_ids(self):
        return [
            self.family_id,
            self.incrementer_family_id,
            self.log_family_id,
            self.cross_edge_family_id,
        ]

    @property
    def fan_out(self) -> np.uint64:
        return self._fan_out

    @property
    def chunk_size(self) -> np.ndarray:
        return self._chunk_size

    @property
    def n_bits_root_counter(self) -> np.ndarray:
        return self._n_bits_root_counter

    @property
    def use_skip_connections(self) -> np.ndarray:
        return self._use_skip_connections

    @property
    def segmentation_chunk_size(self) -> np.ndarray:
        return self.cv.scale["chunk_sizes"][0]

    @property
    def segmentation_resolution(self) -> np.ndarray:
        return np.array(self.cv.scale["resolution"])

    @property
    def segmentation_bounds(self) -> np.ndarray:
        return np.array(self.cv.bounds.to_list()).reshape(2, 3)

    @property
    def n_layers(self) -> int:
        return int(self._n_layers)

    @property
    def bitmasks(self) -> Dict[int, int]:
        return self._bitmasks

    @property
    def cv_mesh_path(self) -> str:
        return "%s/%s" % (self._cv_path, self._mesh_dir)

    @property
    def cv_edges_path(self) -> str:
        return self._edge_dir

    @property
    def dataset_info(self) -> object:
        return self._dataset_info

    @property
    def cv_mip(self) -> int:
        return self._cv_mip

    @property
    def cv(self) -> CloudVolume:
        if self._cv is None:
            self._cv = CloudVolume(
                self._cv_path, mip=self._cv_mip, info=self.dataset_info
            )
        return self._cv

    @property
    def vx_vol_bounds(self):
        return np.array(self.cv.bounds.to_list()).reshape(2, -1).T

    @property
    def root_chunk_id(self):
        return self.get_chunk_id(layer=int(self.n_layers), x=0, y=0, z=0)

    def _check_and_create_table(self) -> None:
        """ Checks if table exists and creates new one if necessary """
        table_ids = [t.table_id for t in self.instance.list_tables()]

        if not self.table_id in table_ids:
            self.table.create()
            f = self.table.column_family(self.family_id)
            f.create()

            f_inc = self.table.column_family(
                self.incrementer_family_id, gc_rule=MaxVersionsGCRule(1)
            )
            f_inc.create()

            f_log = self.table.column_family(self.log_family_id)
            f_log.create()

            f_ce = self.table.column_family(
                self.cross_edge_family_id, gc_rule=MaxVersionsGCRule(1)
            )
            f_ce.create()

            self.logger.info(f"Table {self.table_id} created")

    def check_and_write_table_parameters(
        self,
        column: column_keys._Column,
        value: Optional[Union[str, np.uint64]] = None,
        required: bool = True,
        is_new: bool = False,
    ) -> Union[str, np.uint64]:
        """ Checks if a parameter already exists in the table. If it already
        exists it returns the stored value, else it stores the given value.
        Storing the given values can be enforced with `is_new`. The function
        raises an exception if no value is passed and the parameter does not
        exist, yet.
        :param column: column_keys._Column
        :param value: Union[str, np.uint64]
        :param required: bool
        :param is_new: bool
        :return: Union[str, np.uint64]
            value
        """
        setting = self.read_byte_row(row_key=row_keys.GraphSettings, columns=column)
        if (not setting or is_new) and value is not None:
            row = self.mutate_row(row_keys.GraphSettings, {column: value})
            self.bulk_write([row])
        elif not setting and value is None:
            assert not required
            return None
        else:
            value = setting[0].value

        return value

    def is_in_bounds(self, coordinate: Sequence[int]):
        """ Checks whether a coordinate is within the segmentation bounds
        :param coordinate: [int, int, int]
        :return bool
        """
        coordinate = np.array(coordinate)
        if np.any(coordinate < self.segmentation_bounds[0]):
            return False
        elif np.any(coordinate > self.segmentation_bounds[1]):
            return False
        else:
            return True

    def get_serialized_info(self, credentials=True):
        """ Rerturns dictionary that can be used to load this ChunkedGraph
        :return: dict
        """
        info = {
            "table_id": self.table_id,
            "instance_id": self.instance_id,
            "project_id": self.project_id,
            "meta": self.meta,
        }
        if credentials:
            try:
                info["credentials"] = self.client.credentials
            except:
                info["credentials"] = self.client._credentials
        return info

    def adjust_vol_coordinates_to_cv(
        self, x: np.int, y: np.int, z: np.int, resolution: Sequence[np.int]
    ):
        resolution = np.array(resolution)
        scaling = np.array(self.cv.resolution / resolution, dtype=np.int)
        x = x / scaling[0] - self.vx_vol_bounds[0, 0]
        y = y / scaling[1] - self.vx_vol_bounds[1, 0]
        z = z / scaling[2] - self.vx_vol_bounds[2, 0]
        return np.array([x, y, z])

    def get_chunk_coordinates_from_vol_coordinates(
        self,
        x: np.int,
        y: np.int,
        z: np.int,
        resolution: Sequence[np.int],
        ceil: bool = False,
        layer: int = 1,
    ) -> np.ndarray:
        """ Translates volume coordinates to chunk_coordinates
        :param x: np.int
        :param y: np.int
        :param z: np.int
        :param resolution: np.ndarray
        :param ceil bool
        :param layer: int
        :return:
        """
        resolution = np.array(resolution)
        scaling = np.array(self.cv.resolution / resolution, dtype=np.int)

        x = (x / scaling[0] - self.vx_vol_bounds[0, 0]) / self.chunk_size[0]
        y = (y / scaling[1] - self.vx_vol_bounds[1, 0]) / self.chunk_size[1]
        z = (z / scaling[2] - self.vx_vol_bounds[2, 0]) / self.chunk_size[2]

        x /= self.fan_out ** (max(layer - 2, 0))
        y /= self.fan_out ** (max(layer - 2, 0))
        z /= self.fan_out ** (max(layer - 2, 0))

        coords = np.array([x, y, z])
        if ceil:
            coords = np.ceil(coords)
        return coords.astype(np.int)

    def get_chunk_layer(self, node_or_chunk_id: np.uint64) -> int:
        """ Extract Layer from Node ID or Chunk ID
        :param node_or_chunk_id: np.uint64
        :return: int
        """
        return int(int(node_or_chunk_id) >> 64 - self._n_bits_for_layer_id)

    def get_chunk_layers(self, node_or_chunk_ids: Sequence[np.uint64]) -> np.ndarray:
        """ Extract Layers from Node IDs or Chunk IDs
        :param node_or_chunk_ids: np.ndarray
        :return: np.ndarray
        """
        if len(node_or_chunk_ids) == 0:
            return np.array([], dtype=np.int)
        return self._get_chunk_layer_vec(node_or_chunk_ids)

    def get_chunk_coordinates(self, node_or_chunk_id: np.uint64) -> np.ndarray:
        """ Extract X, Y and Z coordinate from Node ID or Chunk ID
        :param node_or_chunk_id: np.uint64
        :return: Tuple(int, int, int)
        """
        layer = self.get_chunk_layer(node_or_chunk_id)
        bits_per_dim = self.bitmasks[layer]

        x_offset = 64 - self._n_bits_for_layer_id - bits_per_dim
        y_offset = x_offset - bits_per_dim
        z_offset = y_offset - bits_per_dim

        x = int(node_or_chunk_id) >> x_offset & 2 ** bits_per_dim - 1
        y = int(node_or_chunk_id) >> y_offset & 2 ** bits_per_dim - 1
        z = int(node_or_chunk_id) >> z_offset & 2 ** bits_per_dim - 1
        return np.array([x, y, z])

    def get_chunk_id(
        self,
        node_id: Optional[np.uint64] = None,
        layer: Optional[int] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        z: Optional[int] = None,
    ) -> np.uint64:
        """ (1) Extract Chunk ID from Node ID
            (2) Build Chunk ID from Layer, X, Y and Z components
        :param node_id: np.uint64
        :param layer: int
        :param x: int
        :param y: int
        :param z: int
        :return: np.uint64
        """
        assert node_id is not None or all(v is not None for v in [layer, x, y, z])
        if node_id is not None:
            layer = self.get_chunk_layer(node_id)
        bits_per_dim = self.bitmasks[layer]

        if node_id is not None:
            chunk_offset = 64 - self._n_bits_for_layer_id - 3 * bits_per_dim
            return np.uint64((int(node_id) >> chunk_offset) << chunk_offset)
        return compute_chunk_id(layer, x, y, z, bits_per_dim, self._n_bits_for_layer_id)

    def get_chunk_ids_from_node_ids(self, node_ids: Iterable[np.uint64]) -> np.ndarray:
        """ Extract a list of Chunk IDs from a list of Node IDs
        :param node_ids: np.ndarray(dtype=np.uint64)
        :return: np.ndarray(dtype=np.uint64)
        """
        if len(node_ids) == 0:
            return np.array([], dtype=np.int)
        return self._get_chunk_id_vec(node_ids)

    def get_child_chunk_ids(self, node_or_chunk_id: np.uint64) -> np.ndarray:
        """ Calculates the ids of the children chunks in the next lower layer
        :param node_or_chunk_id: np.uint64
        :return: np.ndarray
        """
        chunk_coords = self.get_chunk_coordinates(node_or_chunk_id)
        chunk_layer = self.get_chunk_layer(node_or_chunk_id)

        if chunk_layer == 1:
            return np.array([])
        elif chunk_layer == 2:
            x, y, z = chunk_coords
            return np.array([self.get_chunk_id(layer=chunk_layer - 1, x=x, y=y, z=z)])
        else:
            chunk_ids = []
            for dcoord in product(*[range(self.fan_out)] * 3):
                x, y, z = chunk_coords * self.fan_out + np.array(dcoord)
                child_chunk_id = self.get_chunk_id(layer=chunk_layer - 1, x=x, y=y, z=z)
                chunk_ids.append(child_chunk_id)

            return np.array(chunk_ids)

    def get_parent_chunk_ids(self, node_or_chunk_id: np.uint64) -> np.ndarray:
        """ Creates list of chunk parent ids
        :param node_or_chunk_id: np.uint64
        :return: np.ndarray
        """
        parent_chunk_layers = range(
            self.get_chunk_layer(node_or_chunk_id) + 1, self.n_layers + 1
        )
        chunk_coord = self.get_chunk_coordinates(node_or_chunk_id)
        parent_chunk_ids = [self.get_chunk_id(node_or_chunk_id)]
        for layer in parent_chunk_layers:
            chunk_coord = chunk_coord // self.fan_out
            parent_chunk_ids.append(
                self.get_chunk_id(
                    layer=layer, x=chunk_coord[0], y=chunk_coord[1], z=chunk_coord[2]
                )
            )
        return np.array(parent_chunk_ids, dtype=np.uint64)

    def get_parent_chunk_id_dict(self, node_or_chunk_id: np.uint64) -> dict:
        """ Creates dict of chunk parent ids
        :param node_or_chunk_id: np.uint64
        :return: dict
        """
        chunk_layer = self.get_chunk_layer(node_or_chunk_id)
        return dict(
            zip(
                range(chunk_layer, self.n_layers + 1),
                self.get_parent_chunk_ids(node_or_chunk_id),
            )
        )

    def get_segment_id_limit(self, node_or_chunk_id: np.uint64) -> np.uint64:
        """ Get maximum possible Segment ID for given Node ID or Chunk ID
        :param node_or_chunk_id: np.uint64
        :return: np.uint64
        """
        layer = self.get_chunk_layer(node_or_chunk_id)
        bits_per_dim = self.bitmasks[layer]
        chunk_offset = 64 - self._n_bits_for_layer_id - 3 * bits_per_dim
        return np.uint64(2 ** chunk_offset - 1)

    def get_segment_id(self, node_id: np.uint64) -> np.uint64:
        """ Extract Segment ID from Node ID
        :param node_id: np.uint64
        :return: np.uint64
        """
        return node_id & self.get_segment_id_limit(node_id)

    def get_node_id(
        self,
        segment_id: np.uint64,
        chunk_id: Optional[np.uint64] = None,
        layer: Optional[int] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        z: Optional[int] = None,
    ) -> np.uint64:
        """ (1) Build Node ID from Segment ID and Chunk ID
            (2) Build Node ID from Segment ID, Layer, X, Y and Z components
        :param segment_id: np.uint64
        :param chunk_id: np.uint64
        :param layer: int
        :param x: int
        :param y: int
        :param z: int
        :return: np.uint64
        """
        if chunk_id is not None:
            return chunk_id | segment_id
        else:
            return self.get_chunk_id(layer=layer, x=x, y=y, z=z) | segment_id

    def _get_unique_range(self, row_key, step):
        column = column_keys.Concurrency.CounterID
        # Incrementer row keys start with an "i" followed by the chunk id
        append_row = self.table.row(row_key, append=True)
        append_row.increment_cell_value(column.family_id, column.key, step)

        # This increments the row entry and returns the value AFTER incrementing
        latest_row = append_row.commit()
        max_segment_id = column.deserialize(
            latest_row[column.family_id][column.key][0][0]
        )
        min_segment_id = max_segment_id + np.uint64(1) - step
        return min_segment_id, max_segment_id

    def get_unique_segment_id_root_row(
        self, step: int = 1, counter_id: int = None
    ) -> np.ndarray:
        """ Return unique Segment ID for the Root Chunk
        atomic counter
        :param step: int
        :param counter_id: np.uint64
        :return: np.uint64
        """
        if self.n_bits_root_counter == 0:
            return self.get_unique_segment_id_range(self.root_chunk_id, step=step)

        n_counters = np.uint64(2 ** self._n_bits_root_counter)
        if counter_id is None:
            counter_id = np.uint64(np.random.randint(0, n_counters))
        else:
            counter_id = np.uint64(counter_id % n_counters)

        row_key = serializers.serialize_key(
            f"i{serializers.pad_node_id(self.root_chunk_id)}_{counter_id}"
        )
        min_segment_id, max_segment_id = self._get_unique_range(
            row_key=row_key, step=step
        )

        segment_id_range = np.arange(
            min_segment_id * n_counters + counter_id,
            max_segment_id * n_counters + np.uint64(1) + counter_id,
            n_counters,
            dtype=basetypes.SEGMENT_ID,
        )
        return segment_id_range

    def get_unique_segment_id_range(
        self, chunk_id: np.uint64, step: int = 1
    ) -> np.ndarray:
        """ Return unique Segment ID for given Chunk ID
        atomic counter
        :param chunk_id: np.uint64
        :param step: int
        :return: np.uint64
        """
        if (
            self.n_layers == self.get_chunk_layer(chunk_id)
            and self.n_bits_root_counter > 0
        ):
            return self.get_unique_segment_id_root_row(step=step)

        row_key = serializers.serialize_key("i%s" % serializers.pad_node_id(chunk_id))
        min_segment_id, max_segment_id = self._get_unique_range(
            row_key=row_key, step=step
        )
        segment_id_range = np.arange(
            min_segment_id, max_segment_id + np.uint64(1), dtype=basetypes.SEGMENT_ID
        )
        return segment_id_range

    def get_unique_segment_id(self, chunk_id: np.uint64) -> np.uint64:
        """ Return unique Segment ID for given Chunk ID atomic counter
        :param chunk_id: np.uint64
        :param step: int
        :return: np.uint64
        """
        return self.get_unique_segment_id_range(chunk_id=chunk_id, step=1)[0]

    def get_unique_node_id_range(
        self, chunk_id: np.uint64, step: int = 1
    ) -> np.ndarray:
        """ Return unique Node ID range for given Chunk ID atomic counter
        :param chunk_id: np.uint64
        :param step: int
        :return: np.uint64
        """
        segment_ids = self.get_unique_segment_id_range(chunk_id=chunk_id, step=step)
        node_ids = np.array(
            [self.get_node_id(segment_id, chunk_id) for segment_id in segment_ids],
            dtype=np.uint64,
        )
        return node_ids

    def get_unique_node_id(self, chunk_id: np.uint64) -> np.uint64:
        """ Return unique Node ID for given Chunk ID atomic counter
        :param chunk_id: np.uint64
        :return: np.uint64
        """
        return self.get_unique_node_id_range(chunk_id=chunk_id, step=1)[0]

    def get_max_seg_id_root_chunk(self) -> np.uint64:
        """  Gets maximal root id based on the atomic counter
        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.
        :return: uint64
        """
        if self.n_bits_root_counter == 0:
            return self.get_max_seg_id(self.root_chunk_id)

        n_counters = np.uint64(2 ** self.n_bits_root_counter)
        max_value = 0
        for counter_id in range(n_counters):
            row_key = serializers.serialize_key(
                f"i{serializers.pad_node_id(self.root_chunk_id)}_{counter_id}"
            )
            row = self.read_byte_row(row_key, columns=column_keys.Concurrency.CounterID)
            counter = basetypes.SEGMENT_ID.type(row[0].value if row else 0) * n_counters
            if counter > max_value:
                max_value = counter
        return max_value

    def get_max_seg_id(self, chunk_id: np.uint64) -> np.uint64:
        """  Gets maximal seg id in a chunk based on the atomic counter
        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.
        :return: uint64
        """
        if (
            self.n_layers == self.get_chunk_layer(chunk_id)
            and self.n_bits_root_counter > 0
        ):
            return self.get_max_seg_id_root_chunk()

        # Incrementer row keys start with an "i"
        row_key = serializers.serialize_key("i%s" % serializers.pad_node_id(chunk_id))
        row = self.read_byte_row(row_key, columns=column_keys.Concurrency.CounterID)

        # Read incrementer value (default to 0) and interpret is as Segment ID
        return basetypes.SEGMENT_ID.type(row[0].value if row else 0)

    def get_max_node_id(self, chunk_id: np.uint64) -> np.uint64:
        """  Gets maximal node id in a chunk based on the atomic counter
        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.
        :return: uint64
        """
        max_seg_id = self.get_max_seg_id(chunk_id)
        return self.get_node_id(segment_id=max_seg_id, chunk_id=chunk_id)

    def get_unique_operation_id(self) -> np.uint64:
        """ Finds a unique operation id atomic counter
        Operations essentially live in layer 0. Even if segmentation ids might
        live in layer 0 one day, they would not collide with the operation ids
        because we write information belonging to operations in a separate
        family id.
        :return: str
        """
        column = column_keys.Concurrency.CounterID
        append_row = self.table.row(row_keys.OperationID, append=True)
        append_row.increment_cell_value(column.family_id, column.key, 1)

        # This increments the row entry and returns the value AFTER incrementing
        latest_row = append_row.commit()
        operation_id_b = latest_row[column.family_id][column.key][0][0]
        operation_id = column.deserialize(operation_id_b)
        return np.uint64(operation_id)

    def get_max_operation_id(self) -> np.int64:
        """  Gets maximal operation id based on the atomic counter
        This is an approximation. It is not guaranteed that all ids smaller or
        equal to this id exists. However, it is guaranteed that no larger id
        exist at the time this function is executed.
        :return: int64
        """
        column = column_keys.Concurrency.CounterID
        row = self.read_byte_row(row_keys.OperationID, columns=column)
        return row[0].value if row else column.basetype(0)

    def get_cross_chunk_edges_layer(self, cross_edges):
        """ Computes the layer in which a cross chunk edge becomes relevant.
        I.e. if a cross chunk edge links two nodes in layer 4 this function
        returns 3.
        :param cross_edges: n x 2 array
            edges between atomic (level 1) node ids
        :return: array of length n
        """
        if len(cross_edges) == 0:
            return np.array([], dtype=np.int)

        cross_chunk_edge_layers = np.ones(len(cross_edges), dtype=np.int)
        cross_edge_coordinates = []
        for cross_edge in cross_edges:
            cross_edge_coordinates.append(
                [
                    self.get_chunk_coordinates(cross_edge[0]),
                    self.get_chunk_coordinates(cross_edge[1]),
                ]
            )

        cross_edge_coordinates = np.array(cross_edge_coordinates, dtype=np.int)
        for _ in range(2, self.n_layers):
            edge_diff = np.sum(
                np.abs(cross_edge_coordinates[:, 0] - cross_edge_coordinates[:, 1]),
                axis=1,
            )
            cross_chunk_edge_layers[edge_diff > 0] += 1
            cross_edge_coordinates = cross_edge_coordinates // self.fan_out
        return cross_chunk_edge_layers

    def get_cross_chunk_edge_dict(self, cross_edges):
        """ Generates a cross chunk edge dict for a list of cross chunk edges
        :param cross_edges: n x 2 array
        :return: dict
        """
        cce_layers = self.get_cross_chunk_edges_layer(cross_edges)
        u_cce_layers = np.unique(cce_layers)
        cross_edge_d = {}
        for l in range(2, self.n_layers):
            cross_edge_d[l] = column_keys.Connectivity.CrossChunkEdge.deserialize(b"")

        val_dict = {}
        for cc_layer in u_cce_layers:
            layer_cross_edges = cross_edges[cce_layers == cc_layer]
            if len(layer_cross_edges) > 0:
                val_dict[
                    column_keys.Connectivity.CrossChunkEdge[cc_layer]
                ] = layer_cross_edges
                cross_edge_d[cc_layer] = layer_cross_edges
        return cross_edge_d

    def read_byte_rows(
        self,
        start_key: Optional[bytes] = None,
        end_key: Optional[bytes] = None,
        end_key_inclusive: bool = False,
        row_keys: Optional[Iterable[bytes]] = None,
        columns: Optional[
            Union[Iterable[column_keys._Column], column_keys._Column]
        ] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        end_time_inclusive: bool = False,
    ) -> Dict[
        bytes,
        Union[
            Dict[column_keys._Column, List[bigtable.row_data.Cell]],
            List[bigtable.row_data.Cell],
        ],
    ]:
        """Main function for reading a row range or non-contiguous row sets from Bigtable using
        `bytes` keys.

        Keyword Arguments:
            start_key {Optional[bytes]} -- The first row to be read, ignored if `row_keys` is set.
                If None, no lower boundary is used. (default: {None})
            end_key {Optional[bytes]} -- The end of the row range, ignored if `row_keys` is set.
                If None, no upper boundary is used. (default: {None})
            end_key_inclusive {bool} -- Whether or not `end_key` itself should be included in the
                request, ignored if `row_keys` is set or `end_key` is None. (default: {False})
            row_keys {Optional[Iterable[bytes]]} -- An `Iterable` containing possibly
                non-contiguous row keys. Takes precedence over `start_key` and `end_key`.
                (default: {None})
            columns {Optional[Union[Iterable[column_keys._Column], column_keys._Column]]} --
                Optional filtering by columns to speed up the query. If `columns` is a single
                column (not iterable), the column key will be omitted from the result.
                (default: {None})
            start_time {Optional[datetime.datetime]} -- Ignore cells with timestamp before
                `start_time`. If None, no lower bound. (default: {None})
            end_time {Optional[datetime.datetime]} -- Ignore cells with timestamp after `end_time`.
                If None, no upper bound. (default: {None})
            end_time_inclusive {bool} -- Whether or not `end_time` itself should be included in the
                request, ignored if `end_time` is None. (default: {False})

        Returns:
            Dict[bytes, Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                              List[bigtable.row_data.Cell]]] --
                Returns a dictionary of `byte` rows as keys. Their value will be a mapping of
                columns to a List of cells (one cell per timestamp). Each cell has a `value`
                property, which returns the deserialized field, and a `timestamp` property, which
                returns the timestamp as `datetime.datetime` object.
                If only a single `column_keys._Column` was requested, the List of cells will be
                attached to the row dictionary directly (skipping the column dictionary).
        """
        # Create filters: Column and Time
        filter_ = get_time_range_and_column_filter(
            columns=columns,
            start_time=start_time,
            end_time=end_time,
            end_inclusive=end_time_inclusive,
        )

        # Create filters: Rows
        row_set = RowSet()
        if row_keys is not None:
            for row_key in row_keys:
                row_set.add_row_key(row_key)
        elif start_key is not None and end_key is not None:
            row_set.add_row_range_from_keys(
                start_key=start_key,
                start_inclusive=True,
                end_key=end_key,
                end_inclusive=end_key_inclusive,
            )
        else:
            raise cg_exceptions.PreconditionError(
                "Need to either provide a valid set of rows, or"
                " both, a start row and an end row."
            )

        # Bigtable read with retries
        rows = self._execute_read(row_set=row_set, row_filter=filter_)
        # Deserialize cells
        for row_key, column_dict in rows.items():
            for column, cell_entries in column_dict.items():
                for cell_entry in cell_entries:
                    cell_entry.value = column.deserialize(cell_entry.value)
            # If no column array was requested, reattach single column's values directly to the row
            if isinstance(columns, column_keys._Column):
                rows[row_key] = cell_entries
        return rows

    def read_byte_row(
        self,
        row_key: bytes,
        columns: Optional[
            Union[Iterable[column_keys._Column], column_keys._Column]
        ] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        end_time_inclusive: bool = False,
    ) -> Union[
        Dict[column_keys._Column, List[bigtable.row_data.Cell]],
        List[bigtable.row_data.Cell],
    ]:
        """Convenience function for reading a single row from Bigtable using its `bytes` keys.
        Arguments:
            row_key {bytes} -- The row to be read.
        Keyword Arguments:
            columns {Optional[Union[Iterable[column_keys._Column], column_keys._Column]]} --
                Optional filtering by columns to speed up the query. If `columns` is a single
                column (not iterable), the column key will be omitted from the result.
                (default: {None})
            start_time {Optional[datetime.datetime]} -- Ignore cells with timestamp before
                `start_time`. If None, no lower bound. (default: {None})
            end_time {Optional[datetime.datetime]} -- Ignore cells with timestamp after `end_time`.
                If None, no upper bound. (default: {None})
            end_time_inclusive {bool} -- Whether or not `end_time` itself should be included in the
                request, ignored if `end_time` is None. (default: {False})
        Returns:
            Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                  List[bigtable.row_data.Cell]] --
                Returns a mapping of columns to a List of cells (one cell per timestamp). Each cell
                has a `value` property, which returns the deserialized field, and a `timestamp`
                property, which returns the timestamp as `datetime.datetime` object.
                If only a single `column_keys._Column` was requested, the List of cells is returned
                directly.
        """
        row = self.read_byte_rows(
            row_keys=[row_key],
            columns=columns,
            start_time=start_time,
            end_time=end_time,
            end_time_inclusive=end_time_inclusive,
        )
        if isinstance(columns, column_keys._Column):
            return row.get(row_key, [])
        else:
            return row.get(row_key, {})

    def read_node_id_rows(
        self,
        start_id: Optional[np.uint64] = None,
        end_id: Optional[np.uint64] = None,
        end_id_inclusive: bool = False,
        node_ids: Optional[Iterable[np.uint64]] = None,
        columns: Optional[
            Union[Iterable[column_keys._Column], column_keys._Column]
        ] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        end_time_inclusive: bool = False,
    ) -> Dict[
        np.uint64,
        Union[
            Dict[column_keys._Column, List[bigtable.row_data.Cell]],
            List[bigtable.row_data.Cell],
        ],
    ]:
        """Convenience function for reading a row range or non-contiguous row sets from Bigtable
        representing NodeIDs.
        Keyword Arguments:
            start_id {Optional[np.uint64]} -- The first row to be read, ignored if `node_ids` is
                set. If None, no lower boundary is used. (default: {None})
            end_id {Optional[np.uint64]} -- The end of the row range, ignored if `node_ids` is set.
                If None, no upper boundary is used. (default: {None})
            end_id_inclusive {bool} -- Whether or not `end_id` itself should be included in the
                request, ignored if `node_ids` is set or `end_id` is None. (default: {False})
            node_ids {Optional[Iterable[np.uint64]]} -- An `Iterable` containing possibly
                non-contiguous row keys. Takes precedence over `start_id` and `end_id`.
                (default: {None})
            columns {Optional[Union[Iterable[column_keys._Column], column_keys._Column]]} --
                Optional filtering by columns to speed up the query. If `columns` is a single
                column (not iterable), the column key will be omitted from the result.
                (default: {None})
            start_time {Optional[datetime.datetime]} -- Ignore cells with timestamp before
                `start_time`. If None, no lower bound. (default: {None})
            end_time {Optional[datetime.datetime]} -- Ignore cells with timestamp after `end_time`.
                If None, no upper bound. (default: {None})
            end_time_inclusive {bool} -- Whether or not `end_time` itself should be included in the
                request, ignored if `end_time` is None. (default: {False})

        Returns:
            Dict[np.uint64, Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                                  List[bigtable.row_data.Cell]]] --
                Returns a dictionary of NodeID rows as keys. Their value will be a mapping of
                columns to a List of cells (one cell per timestamp). Each cell has a `value`
                property, which returns the deserialized field, and a `timestamp` property, which
                returns the timestamp as `datetime.datetime` object.
                If only a single `column_keys._Column` was requested, the List of cells will be
                attached to the row dictionary directly (skipping the column dictionary).
        """
        to_bytes = serializers.serialize_uint64
        from_bytes = serializers.deserialize_uint64

        # Read rows (convert Node IDs to row_keys)
        rows = self.read_byte_rows(
            start_key=to_bytes(start_id) if start_id is not None else None,
            end_key=to_bytes(end_id) if end_id is not None else None,
            end_key_inclusive=end_id_inclusive,
            row_keys=(to_bytes(node_id) for node_id in node_ids)
            if node_ids is not None
            else None,
            columns=columns,
            start_time=start_time,
            end_time=end_time,
            end_time_inclusive=end_time_inclusive,
        )

        # Convert row_keys back to Node IDs
        return {from_bytes(row_key): data for (row_key, data) in rows.items()}

    def read_node_id_row(
        self,
        node_id: np.uint64,
        columns: Optional[
            Union[Iterable[column_keys._Column], column_keys._Column]
        ] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        end_time_inclusive: bool = False,
    ) -> Union[
        Dict[column_keys._Column, List[bigtable.row_data.Cell]],
        List[bigtable.row_data.Cell],
    ]:
        """Convenience function for reading a single row from Bigtable, representing a NodeID.
        Arguments:
            node_id {np.uint64} -- the NodeID of the row to be read.

        Keyword Arguments:
            columns {Optional[Union[Iterable[column_keys._Column], column_keys._Column]]} --
                Optional filtering by columns to speed up the query. If `columns` is a single
                column (not iterable), the column key will be omitted from the result.
                (default: {None})
            start_time {Optional[datetime.datetime]} -- Ignore cells with timestamp before
                `start_time`. If None, no lower bound. (default: {None})
            end_time {Optional[datetime.datetime]} -- Ignore cells with timestamp after `end_time`.
                If None, no upper bound. (default: {None})
            end_time_inclusive {bool} -- Whether or not `end_time` itself should be included in the
                request, ignored if `end_time` is None. (default: {False})

        Returns:
            Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                  List[bigtable.row_data.Cell]] --
                Returns a mapping of columns to a List of cells (one cell per timestamp). Each cell
                has a `value` property, which returns the deserialized field, and a `timestamp`
                property, which returns the timestamp as `datetime.datetime` object.
                If only a single `column_keys._Column` was requested, the List of cells is returned
                directly.
        """
        return self.read_byte_row(
            row_key=serializers.serialize_uint64(node_id),
            columns=columns,
            start_time=start_time,
            end_time=end_time,
            end_time_inclusive=end_time_inclusive,
        )

    def read_cross_chunk_edges(
        self, node_id: np.uint64, start_layer: int = 2, end_layer: int = None
    ) -> Dict:
        """ Reads the cross chunk edge entry from the table for a given node id
        and formats it as cross edge dict

        :param node_id:
        :param start_layer:
        :param end_layer:
        :return:
        """
        if end_layer is None:
            end_layer = self.n_layers

        if start_layer < 2 or start_layer == self.n_layers:
            return {}

        start_layer = np.max([self.get_chunk_layer(node_id), start_layer])
        assert end_layer > start_layer and end_layer <= self.n_layers

        columns = [
            column_keys.Connectivity.CrossChunkEdge[l]
            for l in range(start_layer, end_layer)
        ]
        row_dict = self.read_node_id_row(node_id, columns=columns)

        cross_edge_dict = {}
        for l in range(start_layer, end_layer):
            col = column_keys.Connectivity.CrossChunkEdge[l]
            if col in row_dict:
                cross_edge_dict[l] = row_dict[col][0].value
            else:
                cross_edge_dict[l] = col.deserialize(b"")
        return cross_edge_dict

    def mutate_row(
        self,
        row_key: bytes,
        val_dict: Dict[column_keys._Column, Any],
        time_stamp: Optional[datetime.datetime] = None,
        isbytes: bool = False,
    ) -> bigtable.row.Row:
        """ Mutates a single row
        :param row_key: serialized bigtable row key
        :param val_dict: Dict[column_keys._TypedColumn: bytes]
        :param time_stamp: None or datetime
        :return: list
        """
        row = self.table.row(row_key)
        for column, value in val_dict.items():
            if not isbytes:
                value = column.serialize(value)
            row.set_cell(
                column_family_id=column.family_id,
                column=column.key,
                value=value,
                timestamp=time_stamp,
            )
        return row

    def bulk_write(
        self,
        rows: Iterable[bigtable.row.DirectRow],
        root_ids: Optional[Union[np.uint64, Iterable[np.uint64]]] = None,
        operation_id: Optional[np.uint64] = None,
        slow_retry: bool = True,
        block_size: int = 2000,
    ):
        """ Writes a list of mutated rows in bulk
        WARNING: If <rows> contains the same row (same row_key) and column
        key two times only the last one is effectively written to the BigTable
        (even when the mutations were applied to different columns)
        --> no versioning!
        :param rows: list
            list of mutated rows
        :param root_ids: list if uint64
        :param operation_id: uint64 or None
            operation_id (or other unique id) that *was* used to lock the root
            the bulk write is only executed if the root is still locked with
            the same id.
        :param slow_retry: bool
        :param block_size: int
        """
        if slow_retry:
            initial = 5
        else:
            initial = 1

        retry_policy = Retry(
            predicate=if_exception_type(
                (Aborted, DeadlineExceeded, ServiceUnavailable)
            ),
            initial=initial,
            maximum=15.0,
            multiplier=2.0,
            deadline=LOCK_EXPIRED_TIME_DELTA.seconds,
        )

        if root_ids is not None and operation_id is not None:
            if isinstance(root_ids, int):
                root_ids = [root_ids]
            if not self.check_and_renew_root_locks(root_ids, operation_id):
                raise cg_exceptions.LockError(
                    f"Root lock renewal failed for operation ID {operation_id}"
                )

        for i_row in range(0, len(rows), block_size):
            status = self.table.mutate_rows(
                rows[i_row : i_row + block_size], retry=retry_policy
            )
            if not all(status):
                raise cg_exceptions.ChunkedGraphError(
                    f"Bulk write failed for operation ID {operation_id}"
                )

    def _execute_read_thread(self, row_set_and_filter: Tuple[RowSet, RowFilter]):
        row_set, row_filter = row_set_and_filter
        if not row_set.row_keys and not row_set.row_ranges:
            # Check for everything falsy, because Bigtable considers even empty
            # lists of row_keys as no upper/lower bound!
            return {}

        range_read = self.table.read_rows(row_set=row_set, filter_=row_filter)
        res = {v.row_key: partial_row_data_to_column_dict(v) for v in range_read}
        return res

    def _execute_read(
        self, row_set: RowSet, row_filter: RowFilter = None
    ) -> Dict[bytes, Dict[column_keys._Column, bigtable.row_data.PartialRowData]]:
        """ Core function to read rows from Bigtable. Uses standard Bigtable retry logic
        :param row_set: BigTable RowSet
        :param row_filter: BigTable RowFilter
        :return: Dict[bytes, Dict[column_keys._Column, bigtable.row_data.PartialRowData]]
        """

        # FIXME: Bigtable limits the length of the serialized request to 512 KiB. We should
        # calculate this properly (range_read.request.SerializeToString()), but this estimate is
        # good enough for now
        max_row_key_count = 20000
        n_subrequests = max(1, int(np.ceil(len(row_set.row_keys) / max_row_key_count)))
        n_threads = min(n_subrequests, 2 * mu.n_cpus)

        row_sets = []
        for i in range(n_subrequests):
            r = RowSet()
            r.row_keys = row_set.row_keys[
                i * max_row_key_count : (i + 1) * max_row_key_count
            ]
            row_sets.append(r)

        # Don't forget the original RowSet's row_ranges
        row_sets[0].row_ranges = row_set.row_ranges

        responses = mu.multithread_func(
            self._execute_read_thread,
            params=((r, row_filter) for r in row_sets),
            debug=n_threads == 1,
            n_threads=n_threads,
        )

        combined_response = {}
        for resp in responses:
            combined_response.update(resp)

        return combined_response

    def range_read_chunk(
        self,
        layer: Optional[int] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        z: Optional[int] = None,
        chunk_id: Optional[np.uint64] = None,
        columns: Optional[
            Union[Iterable[column_keys._Column], column_keys._Column]
        ] = None,
        time_stamp: Optional[datetime.datetime] = None,
    ) -> Dict[
        np.uint64,
        Union[
            Dict[column_keys._Column, List[bigtable.row_data.Cell]],
            List[bigtable.row_data.Cell],
        ],
    ]:
        """Convenience function for reading all NodeID rows of a single chunk from Bigtable.
        Chunk can either be specified by its (layer, x, y, and z coordinate), or by the chunk ID.

        Keyword Arguments:
            layer {Optional[int]} -- The layer of the chunk within the graph (default: {None})
            x {Optional[int]} -- The xth chunk in x dimension within the graph, within `layer`.
                (default: {None})
            y {Optional[int]} -- The yth chunk in y dimension within the graph, within `layer`.
                (default: {None})
            z {Optional[int]} -- The zth chunk in z dimension within the graph, within `layer`.
                (default: {None})
            chunk_id {Optional[np.uint64]} -- Alternative way to specify the chunk, if the Chunk ID
                is already known. (default: {None})
            columns {Optional[Union[Iterable[column_keys._Column], column_keys._Column]]} --
                Optional filtering by columns to speed up the query. If `columns` is a single
                column (not iterable), the column key will be omitted from the result.
                (default: {None})
            time_stamp {Optional[datetime.datetime]} -- Ignore cells with timestamp after `end_time`.
                If None, no upper bound. (default: {None})

        Returns:
            Dict[np.uint64, Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                                  List[bigtable.row_data.Cell]]] --
                Returns a dictionary of NodeID rows as keys. Their value will be a mapping of
                columns to a List of cells (one cell per timestamp). Each cell has a `value`
                property, which returns the deserialized field, and a `timestamp` property, which
                returns the timestamp as `datetime.datetime` object.
                If only a single `column_keys._Column` was requested, the List of cells will be
                attached to the row dictionary directly (skipping the column dictionary).
        """
        if chunk_id is not None:
            x, y, z = self.get_chunk_coordinates(chunk_id)
            layer = self.get_chunk_layer(chunk_id)
        elif layer is not None and x is not None and y is not None and z is not None:
            chunk_id = self.get_chunk_id(layer=layer, x=x, y=y, z=z)
        else:
            raise Exception(
                "Either chunk_id or layer and coordinates have to be defined"
            )

        if layer == 1:
            max_segment_id = self.get_segment_id_limit(chunk_id)
        else:
            max_segment_id = self.get_max_seg_id(chunk_id=chunk_id)

        # Define BigTable keys
        start_id = self.get_node_id(np.uint64(0), chunk_id=chunk_id)
        end_id = self.get_node_id(max_segment_id, chunk_id=chunk_id)

        try:
            rr = self.read_node_id_rows(
                start_id=start_id,
                end_id=end_id,
                end_id_inclusive=True,
                columns=columns,
                end_time=time_stamp,
                end_time_inclusive=True,
            )
        except Exception as err:
            raise Exception(
                "Unable to consume chunk read: "
                "[%d, %d, %d], l = %d: %s" % (x, y, z, layer, err)
            )
        return rr

    def range_read_layer(self, layer_id: int):
        """ Reads all ids within a layer
        This can take a while depending on the size of the graph
        :param layer_id: int
        :return: list of rows
        """
        raise NotImplementedError()

    def test_if_nodes_are_in_same_chunk(self, node_ids: Sequence[np.uint64]) -> bool:
        """ Test whether two nodes are in the same chunk

        :param node_ids: list of two ints
        :return: bool
        """
        assert len(node_ids) == 2
        return self.get_chunk_id(node_id=node_ids[0]) == self.get_chunk_id(
            node_id=node_ids[1]
        )

    def get_chunk_id_from_coord(self, layer: int, x: int, y: int, z: int) -> np.uint64:
        """ Return ChunkID for given chunked graph layer and voxel coordinates.
        :param layer: int -- ChunkedGraph layer
        :param x: int -- X coordinate in voxel
        :param y: int -- Y coordinate in voxel
        :param z: int -- Z coordinate in voxel
        :return: np.uint64 -- ChunkID
        """
        base_chunk_span = int(self.fan_out) ** max(0, layer - 2)

        return self.get_chunk_id(
            layer=layer,
            x=x // (int(self.chunk_size[0]) * base_chunk_span),
            y=y // (int(self.chunk_size[1]) * base_chunk_span),
            z=z // (int(self.chunk_size[2]) * base_chunk_span),
        )

    def get_atomic_id_from_coord(
        self, x: int, y: int, z: int, parent_id: np.uint64, n_tries: int = 5
    ) -> np.uint64:
        """ Determines atomic id given a coordinate
        :param x: int
        :param y: int
        :param z: int
        :param parent_id: np.uint64
        :param n_tries: int
        :return: np.uint64 or None
        """
        if self.get_chunk_layer(parent_id) == 1:
            return parent_id

        x /= 2 ** self.cv_mip
        y /= 2 ** self.cv_mip

        x = int(x)
        y = int(y)
        z = int(z)

        checked = []
        atomic_id = None
        root_id = self.get_root(parent_id)

        for i_try in range(n_tries):
            # Define block size -- increase by one each try
            x_l = x - (i_try - 1) ** 2
            y_l = y - (i_try - 1) ** 2
            z_l = z - (i_try - 1) ** 2

            x_h = x + 1 + (i_try - 1) ** 2
            y_h = y + 1 + (i_try - 1) ** 2
            z_h = z + 1 + (i_try - 1) ** 2

            x_l = 0 if x_l < 0 else x_l
            y_l = 0 if y_l < 0 else y_l
            z_l = 0 if z_l < 0 else z_l

            # Get atomic ids from cloudvolume
            atomic_id_block = self.cv[x_l:x_h, y_l:y_h, z_l:z_h]
            atomic_ids, atomic_id_count = np.unique(atomic_id_block, return_counts=True)

            # sort by frequency and discard those ids that have been checked
            # previously
            sorted_atomic_ids = atomic_ids[np.argsort(atomic_id_count)]
            sorted_atomic_ids = sorted_atomic_ids[~np.in1d(sorted_atomic_ids, checked)]

            # For each candidate id check whether its root id corresponds to the
            # given root id
            for candidate_atomic_id in sorted_atomic_ids:
                ass_root_id = self.get_root(candidate_atomic_id)
                if ass_root_id == root_id:
                    # atomic_id is not None will be our indicator that the
                    # search was successful
                    atomic_id = candidate_atomic_id
                    break
                else:
                    checked.append(candidate_atomic_id)
            if atomic_id is not None:
                break
        # Returns None if unsuccessful
        return atomic_id

    def read_log_row(
        self, operation_id: np.uint64
    ) -> Dict[column_keys._Column, Union[np.ndarray, np.number]]:
        """ Retrieves log record from Bigtable for a given operation ID
        :param operation_id: np.uint64
        :return: Dict[column_keys._Column, Union[np.ndarray, np.number]]
        """
        columns = [
            column_keys.OperationLogs.UndoOperationID,
            column_keys.OperationLogs.RedoOperationID,
            column_keys.OperationLogs.UserID,
            column_keys.OperationLogs.RootID,
            column_keys.OperationLogs.SinkID,
            column_keys.OperationLogs.SourceID,
            column_keys.OperationLogs.SourceCoordinate,
            column_keys.OperationLogs.SinkCoordinate,
            column_keys.OperationLogs.AddedEdge,
            column_keys.OperationLogs.Affinity,
            column_keys.OperationLogs.RemovedEdge,
            column_keys.OperationLogs.BoundingBoxOffset,
        ]
        log_record = self.read_node_id_row(operation_id, columns=columns)
        log_record.update((column, v[0].value) for column, v in log_record.items())
        return log_record

    def read_first_log_row(self):
        """ Returns first log row
        :return: None or dict
        """
        for operation_id in range(1, 100):
            log_row = self.read_log_row(np.uint64(operation_id))
            if len(log_row) > 0:
                return log_row
        return None

    def add_atomic_edges_in_chunks(
        self,
        edge_id_dict: dict,
        edge_aff_dict: dict,
        edge_area_dict: dict,
        isolated_node_ids: Sequence[np.uint64],
        verbose: bool = True,
        time_stamp: Optional[datetime.datetime] = None,
    ):
        raise NotImplementedError()

    def add_layer(
        self,
        layer_id: int,
        child_chunk_coords: Sequence[Sequence[int]],
        time_stamp: Optional[datetime.datetime] = None,
        verbose: bool = True,
        n_threads: int = 20,
    ) -> None:
        raise NotImplementedError()

    def get_atomic_cross_edge_dict(
        self, node_id: np.uint64, layer_ids: Sequence[int] = None
    ):
        """ Extracts all atomic cross edges and serves them as a dictionary
        :param node_id: np.uint64
        :param layer_ids: list of ints
        :return: dict
        """
        if isinstance(layer_ids, int):
            layer_ids = [layer_ids]

        if layer_ids is None:
            layer_ids = list(range(2, self.n_layers))

        if not layer_ids:
            return {}

        columns = [column_keys.Connectivity.CrossChunkEdge[l] for l in layer_ids]
        row = self.read_node_id_row(node_id, columns=columns)
        if not row:
            return {}

        atomic_cross_edges = {}
        for l in layer_ids:
            column = column_keys.Connectivity.CrossChunkEdge[l]
            atomic_cross_edges[l] = []
            if column in row:
                atomic_cross_edges[l] = row[column][0].value
        return atomic_cross_edges

    def get_parents(
        self,
        node_ids: Sequence[np.uint64],
        get_only_relevant_parents: bool = True,
        time_stamp: Optional[datetime.datetime] = None,
    ):
        """ Acquires parents of a node at a specific time stamp
        :param node_ids: list of uint64
        :param get_only_relevant_parents: bool
            True: return single parent according to time_stamp
            False: return n x 2 list of all parents
                   ((parent_id, time_stamp), ...)
        :param time_stamp: datetime or None
        :return: uint64 or None
        """
        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        if time_stamp.tzinfo is None:
            time_stamp = pytz.UTC.localize(time_stamp)

        parent_rows = self.read_node_id_rows(
            node_ids=node_ids,
            columns=column_keys.Hierarchy.Parent,
            end_time=time_stamp,
            end_time_inclusive=True,
        )

        if not parent_rows:
            return None

        if get_only_relevant_parents:
            return np.array([parent_rows[node_id][0].value for node_id in node_ids])

        parents = []
        for node_id in node_ids:
            parents.append([(p.value, p.timestamp) for p in parent_rows[node_id]])

        return parents

    def get_parent(
        self,
        node_id: np.uint64,
        get_only_relevant_parent: bool = True,
        time_stamp: Optional[datetime.datetime] = None,
    ) -> Union[List[Tuple[np.uint64, datetime.datetime]], np.uint64]:
        """ Acquires parent of a node at a specific time stamp
        :param node_id: uint64
        :param get_only_relevant_parent: bool
            True: return single parent according to time_stamp
            False: return n x 2 list of all parents
                   ((parent_id, time_stamp), ...)
        :param time_stamp: datetime or None
        :return: uint64 or None
        """
        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        if time_stamp.tzinfo is None:
            time_stamp = pytz.UTC.localize(time_stamp)

        parents = self.read_node_id_row(
            node_id,
            columns=column_keys.Hierarchy.Parent,
            end_time=time_stamp,
            end_time_inclusive=True,
        )

        if not parents:
            return None

        if get_only_relevant_parent:
            return parents[0].value
        return [(p.value, p.timestamp) for p in parents]

    def get_children(
        self, node_id: Union[Iterable[np.uint64], np.uint64], flatten: bool = False
    ) -> Union[Dict[np.uint64, np.ndarray], np.ndarray]:
        """Returns children for the specified NodeID or NodeIDs
        :param node_id: The NodeID or NodeIDs for which to retrieve children
        :type node_id: Union[Iterable[np.uint64], np.uint64]
        :param flatten: If True, combine all children into a single array, else generate a map
            of input ``node_id`` to their respective children.
        :type flatten: bool, default is True
        :return: Children for each requested NodeID. The return type depends on the ``flatten``
            parameter.
        :rtype: Union[Dict[np.uint64, np.ndarray], np.ndarray]
        """
        if np.isscalar(node_id):
            children = self.read_node_id_row(
                node_id=node_id, columns=column_keys.Hierarchy.Child
            )
            if not children:
                return np.empty(0, dtype=basetypes.NODE_ID)
            return children[0].value
        else:
            children = self.read_node_id_rows(
                node_ids=node_id, columns=column_keys.Hierarchy.Child
            )
            if flatten:
                if not children:
                    return np.empty(0, dtype=basetypes.NODE_ID)
                return np.concatenate([x[0].value for x in children.values()])
            return {
                x: children[x][0].value
                if x in children
                else np.empty(0, dtype=basetypes.NODE_ID)
                for x in node_id
            }

    def get_latest_roots(
        self,
        time_stamp: Optional[datetime.datetime] = get_max_time(),
        n_threads: int = 1,
    ) -> Sequence[np.uint64]:
        """ Reads _all_ root ids
        :param time_stamp: datetime.datetime
        :param n_threads: int
        :return: array of np.uint64
        """
        return misc.get_latest_roots(
            self, time_stamp=time_stamp, n_threads=n_threads
        )

    def get_delta_roots(
        self,
        time_stamp_start: datetime.datetime,
        time_stamp_end: Optional[datetime.datetime] = None,
        min_seg_id: int = 1,
        n_threads: int = 1,
    ) -> Sequence[np.uint64]:
        """ Returns root ids that have expired or have been created between two timestamps
        :param time_stamp_start: datetime.datetime
            starting timestamp to return deltas from
        :param time_stamp_end: datetime.datetime
            ending timestamp to return deltasfrom
        :param min_seg_id: int (default=1)
            only search from this seg_id and higher (note not a node_id.. use get_seg_id)
        :param n_threads: int (default=1)
            number of threads to use in performing search
        :return new_ids, expired_ids: np.arrays of np.uint64
            new_ids is an array of root_ids for roots that were created after time_stamp_start
            and are still current as of time_stamp_end.
            expired_ids is list of node_id's for roots the expired after time_stamp_start
            but before time_stamp_end.
        """
        return misc.get_delta_roots(
            self,
            time_stamp_start=time_stamp_start,
            time_stamp_end=time_stamp_end,
            min_seg_id=min_seg_id,
            n_threads=n_threads,
        )

    def get_roots(
        self,
        node_ids: Sequence[np.uint64],
        time_stamp: Optional[datetime.datetime] = None,
        stop_layer: int = None,
        n_tries: int = 1,
    ):
        """ Takes node ids and returns the associated agglomeration ids
        :param node_ids: list of uint64
        :param time_stamp: None or datetime
        :return: np.uint64
        """
        time_stamp = get_valid_timestamp(time_stamp)
        stop_layer = self.n_layers if not stop_layer else min(self.n_layers, stop_layer)
        layer_mask = np.ones(len(node_ids), dtype=np.bool)

        for _ in range(n_tries):
            layer_mask[self.get_chunk_layers(node_ids) >= stop_layer] = False
            parent_ids = np.array(node_ids, dtype=basetypes.NODE_ID)
            for _ in range(int(stop_layer + 1)):
                filtered_ids = parent_ids[layer_mask]
                unique_ids, inverse = np.unique(filtered_ids, return_inverse=True)
                temp_ids = self.get_parents(unique_ids, time_stamp=time_stamp)
                if temp_ids is None:
                    break
                else:
                    parent_ids[layer_mask] = temp_ids[inverse]
                    layer_mask[self.get_chunk_layers(parent_ids) >= stop_layer] = False
                    if not np.any(self.get_chunk_layers(parent_ids) < stop_layer):
                        return parent_ids
            if not np.any(self.get_chunk_layers(parent_ids) < stop_layer):
                return parent_ids
            else:
                time.sleep(0.5)
        return parent_ids

    def get_root(
        self,
        node_id: np.uint64,
        time_stamp: Optional[datetime.datetime] = None,
        get_all_parents=False,
        stop_layer: int = None,
        n_tries: int = 1,
    ) -> Union[List[np.uint64], np.uint64]:
        """ Takes a node id and returns the associated agglomeration ids
        :param node_id: uint64
        :param time_stamp: None or datetime
        :return: np.uint64
        """
        time_stamp = get_valid_timestamp(time_stamp)
        parent_id = node_id
        all_parent_ids = []

        if stop_layer is not None:
            stop_layer = min(self.n_layers, stop_layer)
        else:
            stop_layer = self.n_layers

        for _ in range(n_tries):
            parent_id = node_id
            for _ in range(self.get_chunk_layer(node_id), int(stop_layer + 1)):
                temp_parent_id = self.get_parent(parent_id, time_stamp=time_stamp)
                if temp_parent_id is None:
                    break
                else:
                    parent_id = temp_parent_id
                    all_parent_ids.append(parent_id)
                    if self.get_chunk_layer(parent_id) >= stop_layer:
                        break
            if self.get_chunk_layer(parent_id) >= stop_layer:
                break
            else:
                time.sleep(0.5)

        if self.get_chunk_layer(parent_id) < stop_layer:
            raise Exception("Cannot find root id {}, {}".format(node_id, time_stamp))

        if get_all_parents:
            return np.array(all_parent_ids)
        else:
            return parent_id

    def get_all_parents_dict(
        self, node_id: np.uint64, time_stamp: Optional[datetime.datetime] = None
    ) -> dict:
        """ Takes a node id and returns all parents and parents' parents up to
            the top
        :param node_id: uint64
        :param time_stamp: None or datetime
        :return: dict
        """
        parent_ids = self.get_root(
            node_id=node_id, time_stamp=time_stamp, get_all_parents=True
        )
        parent_id_layers = self.get_chunk_layers(parent_ids)
        return dict(zip(parent_id_layers, parent_ids))

    def lock_root_loop(
        self,
        root_ids: Sequence[np.uint64],
        operation_id: np.uint64,
        max_tries: int = 1,
        waittime_s: float = 0.5,
    ) -> Tuple[bool, np.ndarray]:
        """ Attempts to lock multiple roots at the same time
        :param root_ids: list of uint64
        :param operation_id: uint64
        :param max_tries: int
        :param waittime_s: float
        :return: bool, list of uint64s
            success, latest root ids
        """
        i_try = 0
        while i_try < max_tries:
            lock_acquired = False
            # Collect latest root ids
            new_root_ids: List[np.uint64] = []
            for i_root_id in range(len(root_ids)):
                future_root_ids = self.get_future_root_ids(root_ids[i_root_id])
                if len(future_root_ids) == 0:
                    new_root_ids.append(root_ids[i_root_id])
                else:
                    new_root_ids.extend(future_root_ids)

            # Attempt to lock all latest root ids
            root_ids = np.unique(new_root_ids)
            for i_root_id in range(len(root_ids)):
                self.logger.debug(
                    "operation id: %d - root id: %d"
                    % (operation_id, root_ids[i_root_id])
                )
                lock_acquired = self.lock_single_root(root_ids[i_root_id], operation_id)
                # Roll back locks if one root cannot be locked
                if not lock_acquired:
                    for j_root_id in range(len(root_ids)):
                        self.unlock_root(root_ids[j_root_id], operation_id)
                    break

            if lock_acquired:
                return True, root_ids

            time.sleep(waittime_s)
            i_try += 1
            self.logger.debug(f"Try {i_try}")
        return False, root_ids

    def lock_single_root(self, root_id: np.uint64, operation_id: np.uint64) -> bool:
        """ Attempts to lock the latest version of a root node

        :param root_id: uint64
        :param operation_id: uint64
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """
        operation_id_b = serializers.serialize_uint64(operation_id)
        lock_column = column_keys.Concurrency.Lock
        new_parents_column = column_keys.Hierarchy.NewParent

        # Build a column filter which tests if a lock was set (== lock column
        # exists) and if it is still valid (timestamp younger than
        # LOCK_EXPIRED_TIME_DELTA) and if there is no new parent (== new_parents
        # exists)

        time_cutoff = datetime.datetime.utcnow() - LOCK_EXPIRED_TIME_DELTA
        # Comply to resolution of BigTables TimeRange
        time_cutoff -= datetime.timedelta(microseconds=time_cutoff.microsecond % 1000)
        time_filter = TimestampRangeFilter(TimestampRange(start=time_cutoff))
        # lock_key_filter = ColumnQualifierRegexFilter(lock_column.key)
        # new_parents_key_filter = ColumnQualifierRegexFilter(new_parents_column.key)

        lock_key_filter = ColumnRangeFilter(
            column_family_id=lock_column.family_id,
            start_column=lock_column.key,
            end_column=lock_column.key,
            inclusive_start=True,
            inclusive_end=True,
        )

        new_parents_key_filter = ColumnRangeFilter(
            column_family_id=new_parents_column.family_id,
            start_column=new_parents_column.key,
            end_column=new_parents_column.key,
            inclusive_start=True,
            inclusive_end=True,
        )

        # Combine filters together
        chained_filter = RowFilterChain([time_filter, lock_key_filter])
        combined_filter = ConditionalRowFilter(
            base_filter=chained_filter,
            true_filter=PassAllFilter(True),
            false_filter=new_parents_key_filter,
        )

        # Get conditional row using the chained filter
        root_row = self.table.row(
            serializers.serialize_uint64(root_id), filter_=combined_filter
        )

        # Set row lock if condition returns no results (state == False)
        time_stamp = get_valid_timestamp(None)
        root_row.set_cell(
            lock_column.family_id,
            lock_column.key,
            operation_id_b,
            state=False,
            timestamp=time_stamp,
        )

        # The lock was acquired when set_cell returns False (state)
        lock_acquired = not root_row.commit()

        if not lock_acquired:
            row = self.read_node_id_row(root_id, columns=lock_column)
            l_operation_ids = [cell.value for cell in row]
            self.logger.debug(f"Locked operation ids: {l_operation_ids}")
        return lock_acquired

    def unlock_root(self, root_id: np.uint64, operation_id: np.uint64) -> bool:
        """ Unlocks a root
        This is mainly used for cases where multiple roots need to be locked and
        locking was not sucessful for all of them

        :param root_id: np.uint64
        :param operation_id: uint64
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """
        lock_column = column_keys.Concurrency.Lock
        operation_id_b = lock_column.serialize(operation_id)

        # Build a column filter which tests if a lock was set (== lock column
        # exists) and if it is still valid (timestamp younger than
        # LOCK_EXPIRED_TIME_DELTA) and if the given operation_id is still
        # the active lock holder

        time_cutoff = datetime.datetime.utcnow() - LOCK_EXPIRED_TIME_DELTA
        # Comply to resolution of BigTables TimeRange
        time_cutoff -= datetime.timedelta(microseconds=time_cutoff.microsecond % 1000)
        time_filter = TimestampRangeFilter(TimestampRange(start=time_cutoff))

        # column_key_filter = ColumnQualifierRegexFilter(lock_column.key)
        # value_filter = ColumnQualifierRegexFilter(operation_id_b)

        column_key_filter = ColumnRangeFilter(
            column_family_id=lock_column.family_id,
            start_column=lock_column.key,
            end_column=lock_column.key,
            inclusive_start=True,
            inclusive_end=True,
        )

        value_filter = ValueRangeFilter(
            start_value=operation_id_b,
            end_value=operation_id_b,
            inclusive_start=True,
            inclusive_end=True,
        )

        # Chain these filters together
        chained_filter = RowFilterChain([time_filter, column_key_filter, value_filter])

        # Get conditional row using the chained filter
        root_row = self.table.row(
            serializers.serialize_uint64(root_id), filter_=chained_filter
        )

        # Delete row if conditions are met (state == True)
        root_row.delete_cell(lock_column.family_id, lock_column.key, state=True)
        return root_row.commit()

    def check_and_renew_root_locks(
        self, root_ids: Iterable[np.uint64], operation_id: np.uint64
    ) -> bool:
        """ Tests if the roots are locked with the provided operation_id and
        renews the lock to reset the time_stam
        This is mainly used before executing a bulk write
        :param root_ids: uint64
        :param operation_id: uint64
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """
        for root_id in root_ids:
            if not self.check_and_renew_root_lock_single(root_id, operation_id):
                self.logger.warning(f"check_and_renew_root_locks failed - {root_id}")
                return False
        return True

    def check_and_renew_root_lock_single(
        self, root_id: np.uint64, operation_id: np.uint64
    ) -> bool:
        """ Tests if the root is locked with the provided operation_id and
        renews the lock to reset the time_stam
        This is mainly used before executing a bulk write
        :param root_id: uint64
        :param operation_id: uint64
            an id that is unique to the process asking to lock the root node
        :return: bool
            success
        """
        lock_column = column_keys.Concurrency.Lock
        new_parents_column = column_keys.Hierarchy.NewParent
        operation_id_b = lock_column.serialize(operation_id)

        # Build a column filter which tests if a lock was set (== lock column
        # exists) and if the given operation_id is still the active lock holder
        # and there is no new parent (== new_parents column exists). The latter
        # is not necessary but we include it as a backup to prevent things
        # from going really bad.

        # column_key_filter = ColumnQualifierRegexFilter(lock_column.key)
        # value_filter = ColumnQualifierRegexFilter(operation_id_b)
        column_key_filter = ColumnRangeFilter(
            column_family_id=lock_column.family_id,
            start_column=lock_column.key,
            end_column=lock_column.key,
            inclusive_start=True,
            inclusive_end=True,
        )

        value_filter = ValueRangeFilter(
            start_value=operation_id_b,
            end_value=operation_id_b,
            inclusive_start=True,
            inclusive_end=True,
        )

        new_parents_key_filter = ColumnRangeFilter(
            column_family_id=self.family_id,
            start_column=new_parents_column.key,
            end_column=new_parents_column.key,
            inclusive_start=True,
            inclusive_end=True,
        )

        # Chain these filters together
        chained_filter = RowFilterChain([column_key_filter, value_filter])
        combined_filter = ConditionalRowFilter(
            base_filter=chained_filter,
            true_filter=new_parents_key_filter,
            false_filter=PassAllFilter(True),
        )

        # Get conditional row using the chained filter
        root_row = self.table.row(
            serializers.serialize_uint64(root_id), filter_=combined_filter
        )

        # Set row lock if condition returns a result (state == True)
        root_row.set_cell(
            lock_column.family_id, lock_column.key, operation_id_b, state=False
        )

        # The lock was acquired when set_cell returns True (state)
        lock_acquired = not root_row.commit()
        return lock_acquired

    def read_consolidated_lock_timestamp(
        self, root_ids: Sequence[np.uint64], operation_ids: Sequence[np.uint64]
    ) -> Union[datetime.datetime, None]:
        """ Returns minimum of many lock timestamps
        :param root_ids: np.ndarray
        :param operation_ids: np.ndarray
        :return:
        """
        time_stamps = []
        for root_id, operation_id in zip(root_ids, operation_ids):
            time_stamp = self.read_lock_timestamp(root_id, operation_id)
            if time_stamp is None:
                return None
            time_stamps.append(time_stamp)
        if len(time_stamps) == 0:
            return None
        return np.min(time_stamps)

    def read_lock_timestamp(
        self, root_id: np.uint64, operation_id: np.uint64
    ) -> Union[datetime.datetime, None]:
        """ Reads timestamp from lock row to get a consistent timestamp across
            multiple nodes / pods
        :param root_id: np.uint64
        :param operation_id: np.uint64
            Checks whether the root_id is actually locked with this operation_id
        :return: datetime.datetime or None
        """
        row = self.read_node_id_row(root_id, columns=column_keys.Concurrency.Lock)
        if len(row) == 0:
            self.logger.warning(f"No lock found for {root_id}")
            return None

        if row[0].value != operation_id:
            self.logger.warning(f"{root_id} not locked with {operation_id}")
            return None
        return row[0].timestamp

    def get_latest_root_id(self, root_id: np.uint64) -> np.ndarray:
        """ Returns the latest root id associated with the provided root id
        :param root_id: uint64
        :return: list of uint64s
        """
        id_working_set = [root_id]
        column = column_keys.Hierarchy.NewParent
        latest_root_ids = []
        while len(id_working_set) > 0:
            next_id = id_working_set[0]
            del id_working_set[0]
            row = self.read_node_id_row(next_id, columns=column)
            # Check if a new root id was attached to this root id
            if row:
                id_working_set.extend(row[0].value)
            else:
                latest_root_ids.append(next_id)

        return np.unique(latest_root_ids)

    def get_future_root_ids(
        self,
        root_id: np.uint64,
        time_stamp: Optional[datetime.datetime] = get_max_time(),
    ) -> np.ndarray:
        """ Returns all future root ids emerging from this root
        This search happens in a monotic fashion. At no point are past root
        ids of future root ids taken into account.
        :param root_id: np.uint64
        :param time_stamp: None or datetime
            restrict search to ids created before this time_stamp
            None=search whole future
        :return: array of uint64
        """
        time_stamp = get_valid_timestamp(time_stamp)
        id_history = []
        next_ids = [root_id]
        while len(next_ids):
            temp_next_ids = []
            for next_id in next_ids:
                row = self.read_node_id_row(
                    next_id,
                    columns=[
                        column_keys.Hierarchy.NewParent,
                        column_keys.Hierarchy.Child,
                    ],
                )
                if column_keys.Hierarchy.NewParent in row:
                    ids = row[column_keys.Hierarchy.NewParent][0].value
                    row_time_stamp = row[column_keys.Hierarchy.NewParent][0].timestamp
                elif column_keys.Hierarchy.Child in row:
                    ids = None
                    row_time_stamp = row[column_keys.Hierarchy.Child][0].timestamp
                else:
                    raise cg_exceptions.ChunkedGraphError(
                        "Error retrieving future root ID of %s" % next_id
                    )

                if row_time_stamp < time_stamp:
                    if ids is not None:
                        temp_next_ids.extend(ids)
                    if next_id != root_id:
                        id_history.append(next_id)

            next_ids = temp_next_ids
        return np.unique(np.array(id_history, dtype=np.uint64))

    def get_past_root_ids(
        self,
        root_id: np.uint64,
        time_stamp: Optional[datetime.datetime] = get_min_time(),
    ) -> np.ndarray:
        """ Returns all future root ids emerging from this root
        This search happens in a monotic fashion. At no point are future root
        ids of past root ids taken into account.
        :param root_id: np.uint64
        :param time_stamp: None or datetime
            restrict search to ids created after this time_stamp
            None=search whole future
        :return: array of uint64
        """
        time_stamp = get_valid_timestamp(time_stamp)
        id_history = []
        next_ids = [root_id]
        while len(next_ids):
            temp_next_ids = []
            for next_id in next_ids:
                row = self.read_node_id_row(
                    next_id,
                    columns=[
                        column_keys.Hierarchy.FormerParent,
                        column_keys.Hierarchy.Child,
                    ],
                )
                if column_keys.Hierarchy.FormerParent in row:
                    ids = row[column_keys.Hierarchy.FormerParent][0].value
                    row_time_stamp = row[column_keys.Hierarchy.FormerParent][
                        0
                    ].timestamp
                elif column_keys.Hierarchy.Child in row:
                    ids = None
                    row_time_stamp = row[column_keys.Hierarchy.Child][0].timestamp
                else:
                    raise cg_exceptions.ChunkedGraphError(
                        "Error retrieving past root ID of %s" % next_id
                    )

                if row_time_stamp > time_stamp:
                    if ids is not None:
                        temp_next_ids.extend(ids)

                    if next_id != root_id:
                        id_history.append(next_id)

            next_ids = temp_next_ids
        return np.unique(np.array(id_history, dtype=np.uint64))

    def get_root_id_history(
        self,
        root_id: np.uint64,
        time_stamp_past: Optional[datetime.datetime] = get_min_time(),
        time_stamp_future: Optional[datetime.datetime] = get_max_time(),
    ) -> np.ndarray:
        """ Returns all future root ids emerging from this root
        This search happens in a monotic fashion. At no point are future root
        ids of past root ids or past root ids of future root ids taken into
        account.
        :param root_id: np.uint64
        :param time_stamp_past: None or datetime
            restrict search to ids created after this time_stamp
            None=search whole future
        :param time_stamp_future: None or datetime
            restrict search to ids created before this time_stamp
            None=search whole future
        :return: array of uint64
        """
        past_ids = self.get_past_root_ids(root_id=root_id, time_stamp=time_stamp_past)
        future_ids = self.get_future_root_ids(
            root_id=root_id, time_stamp=time_stamp_future
        )

        history_ids = np.concatenate(
            [past_ids, np.array([root_id], dtype=np.uint64), future_ids]
        )
        return history_ids

    def get_change_log(
        self,
        root_id: np.uint64,
        correct_for_wrong_coord_type: bool = True,
        time_stamp_past: Optional[datetime.datetime] = get_min_time(),
    ) -> dict:
        """ Returns all past root ids for this root
        This search happens in a monotic fashion. At no point are future root
        ids of past root ids taken into account.
        :param root_id: np.uint64
        :param correct_for_wrong_coord_type: bool
            pinky100? --> True
        :param time_stamp_past: None or datetime
            restrict search to ids created after this time_stamp
            None=search whole past
        :return: past ids, merge sv ids, merge edge coords, split sv ids
        """
        if time_stamp_past.tzinfo is None:
            time_stamp_past = pytz.UTC.localize(time_stamp_past)

        id_history = []
        merge_history = []
        merge_history_edges = []
        split_history = []

        next_ids = [root_id]
        while len(next_ids):
            temp_next_ids = []
            former_parent_col = column_keys.Hierarchy.FormerParent
            row_dict = self.read_node_id_rows(
                node_ids=next_ids, columns=[former_parent_col]
            )
            for row in row_dict.values():
                if column_keys.Hierarchy.FormerParent in row:
                    if time_stamp_past > row[former_parent_col][0].timestamp:
                        continue
                    ids = row[former_parent_col][0].value
                    lock_col = column_keys.Concurrency.Lock
                    former_row = self.read_node_id_row(ids[0], columns=[lock_col])
                    operation_id = former_row[lock_col][0].value
                    log_row = self.read_log_row(operation_id)
                    is_merge = column_keys.OperationLogs.AddedEdge in log_row

                    for id_ in ids:
                        if id_ in id_history:
                            continue
                        id_history.append(id_)
                        temp_next_ids.append(id_)

                    if is_merge:
                        added_edges = log_row[column_keys.OperationLogs.AddedEdge]
                        merge_history.append(added_edges)
                        coords = [
                            log_row[column_keys.OperationLogs.SourceCoordinate],
                            log_row[column_keys.OperationLogs.SinkCoordinate],
                        ]

                        if correct_for_wrong_coord_type:
                            # A little hack because we got the datatype wrong...
                            coords = [
                                np.frombuffer(coords[0]),
                                np.frombuffer(coords[1]),
                            ]
                            coords *= self.segmentation_resolution

                        merge_history_edges.append(coords)

                    if not is_merge:
                        removed_edges = log_row[column_keys.OperationLogs.RemovedEdge]
                        split_history.append(removed_edges)
                else:
                    continue

            next_ids = temp_next_ids
        return {
            "past_ids": np.unique(np.array(id_history, dtype=np.uint64)),
            "merge_edges": np.array(merge_history),
            "merge_edge_coords": np.array(merge_history_edges),
            "split_edges": np.array(split_history),
        }

    def normalize_bounding_box(
        self, bounding_box: Optional[Sequence[Sequence[int]]], bb_is_coordinate: bool
    ) -> Union[Sequence[Sequence[int]], None]:
        if bounding_box is None:
            return None

        if bb_is_coordinate:
            bounding_box[0] = self.get_chunk_coordinates_from_vol_coordinates(
                bounding_box[0][0],
                bounding_box[0][1],
                bounding_box[0][2],
                resolution=self.cv.resolution,
                ceil=False,
            )
            bounding_box[1] = self.get_chunk_coordinates_from_vol_coordinates(
                bounding_box[1][0],
                bounding_box[1][1],
                bounding_box[1][2],
                resolution=self.cv.resolution,
                ceil=True,
            )
            return bounding_box
        else:
            return np.array(bounding_box, dtype=np.int)

    def _get_subgraph_higher_layer_nodes(
        self,
        node_id: np.uint64,
        bounding_box: Optional[Sequence[Sequence[int]]],
        return_layers: Sequence[int],
        verbose: bool,
    ):
        def _get_subgraph_higher_layer_nodes_threaded(
            node_ids: Iterable[np.uint64],
        ) -> List[np.uint64]:
            children = self.get_children(node_ids, flatten=True)
            if len(children) > 0 and bounding_box is not None:
                chunk_coordinates = np.array(
                    [self.get_chunk_coordinates(c) for c in children]
                )
                child_layers = self.get_chunk_layers(children)
                adapt_child_layers = child_layers - 2
                adapt_child_layers[adapt_child_layers < 0] = 0
                bounding_box_layer = (
                    bounding_box[None]
                    / (self.fan_out ** adapt_child_layers)[:, None, None]
                )
                bound_check = np.array(
                    [
                        np.all(chunk_coordinates < bounding_box_layer[:, 1], axis=1),
                        np.all(
                            chunk_coordinates + 1 > bounding_box_layer[:, 0], axis=1
                        ),
                    ]
                ).T

                bound_check_mask = np.all(bound_check, axis=1)
                children = children[bound_check_mask]
            return children

        if bounding_box is not None:
            bounding_box = np.array(bounding_box)

        layer = self.get_chunk_layer(node_id)
        assert layer > 1

        nodes_per_layer = {}
        child_ids = np.array([node_id], dtype=np.uint64)
        stop_layer = max(2, np.min(return_layers))

        if layer in return_layers:
            nodes_per_layer[layer] = child_ids

        if verbose:
            time_start = time.time()

        while layer > stop_layer:
            # Use heuristic to guess the optimal number of threads
            child_id_layers = self.get_chunk_layers(child_ids)
            this_layer_m = child_id_layers == layer
            this_layer_child_ids = child_ids[this_layer_m]
            next_layer_child_ids = child_ids[~this_layer_m]

            n_child_ids = len(child_ids)
            this_n_threads = np.min([int(n_child_ids // 50000) + 1, mu.n_cpus])

            child_ids = np.fromiter(
                chain.from_iterable(
                    mu.multithread_func(
                        _get_subgraph_higher_layer_nodes_threaded,
                        np.array_split(this_layer_child_ids, this_n_threads),
                        n_threads=this_n_threads,
                        debug=this_n_threads == 1,
                    )
                ),
                np.uint64,
            )
            child_ids = np.concatenate([child_ids, next_layer_child_ids])

            if verbose:
                self.logger.debug(
                    "Layer %d: %.3fms for %d children with %d threads"
                    % (
                        layer,
                        (time.time() - time_start) * 1000,
                        n_child_ids,
                        this_n_threads,
                    )
                )
                time_start = time.time()

            layer -= 1
            if layer in return_layers:
                nodes_per_layer[layer] = child_ids
        return nodes_per_layer

    def get_subgraph_edges(
        self,
        agglomeration_id: np.uint64,
        bounding_box: Optional[Sequence[Sequence[int]]] = None,
        bb_is_coordinate: bool = False,
        connected_edges=True,
        verbose: bool = True,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """ 
        Return all atomic edges between supervoxels belonging to the 
        specified agglomeration ID within the defined bounding box
        """
        return self.get_subgraph(
            np.array([agglomeration_id]),
            bbox=bounding_box,
            bbox_is_coordinate=bb_is_coordinate,
        )

    def get_subgraph(
        self,
        agglomeration_ids: np.ndarray,
        bbox: Optional[Sequence[Sequence[int]]] = None,
        bbox_is_coordinate: bool = False,
        cv_threads: int = 1,
        active_edges: bool = True,
        timestamp: datetime.datetime = None,
    ) -> Tuple[Dict, Dict]:
        """
        1. get level 2 children ids belonging to the agglomerations
        2. get relevant chunk ids from level 2 ids
        3. read edges from cloud storage (include fake edges from big table)
        4. get supervoxel ids from level 2 ids
        5. filter the edges with supervoxel ids
        6. optionally for each edge (v1,v2) active
           if parent(v1) == parent(v2) inactive otherwise
        7. returns dict of Agglomerations
        """

        def _read_edges(chunk_ids) -> dict:
            return get_chunk_edges(
                self.cv_edges_path,
                [self.get_chunk_coordinates(chunk_id) for chunk_id in chunk_ids],
                cv_threads,
            )

        level2_ids = []
        for agglomeration_id in agglomeration_ids:
            layer_nodes_d = self._get_subgraph_higher_layer_nodes(
                node_id=agglomeration_id,
                bounding_box=self.normalize_bounding_box(bbox, bbox_is_coordinate),
                return_layers=[2],
                verbose=False,
            )
            level2_ids.append(layer_nodes_d[2])
        level2_ids = np.concatenate(level2_ids)

        chunk_ids = self.get_chunk_ids_from_node_ids(level2_ids)
        cg_threads = 1
        chunk_edge_dicts = mu.multithread_func(
            _read_edges,
            np.array_split(np.unique(chunk_ids), cg_threads),
            n_threads=cg_threads,
            debug=False,
        )
        edges_dict = concatenate_chunk_edges(chunk_edge_dicts)
        edges = reduce(lambda x, y: x + y, edges_dict.values())
        # # include fake edges
        # chunk_fake_edges_d = self.read_node_id_rows(
        #     node_ids=chunk_ids,
        #     columns=column_keys.Connectivity.FakeEdges)
        # fake_edges = np.concatenate([list(chunk_fake_edges_d.values())])
        # if fake_edges.size:
        #     fake_edges = Edges(fake_edges[:,0], fake_edges[:,1])
        #     edges += fake_edges

        # group nodes and edges based on level 2 ids
        l2id_agglomeration_d = {}
        l2id_children_d = self.get_children(level2_ids)
        for l2id in l2id_children_d:
            supervoxels = l2id_children_d[l2id]
            filtered_edges = filter_edges(l2id_children_d[l2id], edges)
            if active_edges:
                filtered_edges = get_active_edges(filtered_edges, l2id_children_d)
            # l2id_agglomeration_d[l2id] = Agglomeration(supervoxels, filtered_edges)
        return l2id_agglomeration_d

    def get_subgraph_nodes(
        self,
        agglomeration_id: np.uint64,
        bounding_box: Optional[Sequence[Sequence[int]]] = None,
        bb_is_coordinate: bool = False,
        return_layers: List[int] = [1],
        verbose: bool = True,
    ) -> Union[Dict[int, np.ndarray], np.ndarray]:
        """ Return all nodes belonging to the specified agglomeration ID within
            the defined bounding box and requested layers.

        :param agglomeration_id: np.uint64
        :param bounding_box: [[x_l, y_l, z_l], [x_h, y_h, z_h]]
        :param bb_is_coordinate: bool
        :param return_layers: List[int]
        :param verbose: bool
        :return: np.array of atomic IDs if single layer is requested,
                 Dict[int, np.array] if multiple layers are requested
        """

        def _get_subgraph_layer2_nodes(node_ids: Iterable[np.uint64]) -> np.ndarray:
            return self.get_children(node_ids, flatten=True)

        stop_layer = np.min(return_layers)
        bounding_box = self.normalize_bounding_box(bounding_box, bb_is_coordinate)

        # Layer 3+
        if stop_layer >= 2:
            nodes_per_layer = self._get_subgraph_higher_layer_nodes(
                node_id=agglomeration_id,
                bounding_box=bounding_box,
                return_layers=return_layers,
                verbose=verbose,
            )
        else:
            # Need to retrieve layer 2 even if the user doesn't require it
            nodes_per_layer = self._get_subgraph_higher_layer_nodes(
                node_id=agglomeration_id,
                bounding_box=bounding_box,
                return_layers=return_layers + [2],
                verbose=verbose,
            )

            # Layer 2
            if verbose:
                time_start = time.time()
            child_ids = nodes_per_layer[2]
            if 2 not in return_layers:
                del nodes_per_layer[2]

            # Use heuristic to guess the optimal number of threads
            n_child_ids = len(child_ids)
            this_n_threads = np.min([int(n_child_ids // 50000) + 1, mu.n_cpus])

            child_ids = np.fromiter(
                chain.from_iterable(
                    mu.multithread_func(
                        _get_subgraph_layer2_nodes,
                        np.array_split(child_ids, this_n_threads),
                        n_threads=this_n_threads,
                        debug=this_n_threads == 1,
                    )
                ),
                dtype=np.uint64,
            )
            if verbose:
                self.logger.debug(
                    "Layer 2: %.3fms for %d children with %d threads"
                    % ((time.time() - time_start) * 1000, n_child_ids, this_n_threads)
                )

            nodes_per_layer[1] = child_ids
        if len(nodes_per_layer) == 1:
            return list(nodes_per_layer.values())[0]
        else:
            return nodes_per_layer

    def flatten_row_dict(
        self, row_dict: Dict[column_keys._Column, List[bigtable.row_data.Cell]]
    ) -> Dict:
        """ Flattens multiple entries to columns by appending them

        :param row_dict: dict
            family key has to be resolved
        :return: dict
        """
        flattened_row_dict = {}
        for column, column_entries in row_dict.items():
            flattened_row_dict[column] = []
            if len(column_entries) > 0:
                for column_entry in column_entries[::-1]:
                    flattened_row_dict[column].append(column_entry.value)

                if np.isscalar(column_entry.value):
                    flattened_row_dict[column] = np.array(flattened_row_dict[column])
                else:
                    flattened_row_dict[column] = np.concatenate(
                        flattened_row_dict[column]
                    )
            else:
                flattened_row_dict[column] = column.deserialize(b"")

            if column == column_keys.Connectivity.Connected:
                u_ids, c_ids = np.unique(flattened_row_dict[column], return_counts=True)
                flattened_row_dict[column] = u_ids[(c_ids % 2) == 1].astype(
                    column.basetype
                )
        return flattened_row_dict

    def get_chunk_split_partners(self, atomic_id: np.uint64):
        """ Finds all atomic nodes beloning to the same supervoxel before
            chunking (affs == inf)
        :param atomic_id: np.uint64
        :return: list of np.uint64
        """
        chunk_split_partners = [atomic_id]
        atomic_ids = [atomic_id]
        while len(atomic_ids) > 0:
            atomic_id = atomic_ids[0]
            del atomic_ids[0]
            partners, affs, _ = self.get_atomic_partners(
                atomic_id,
                include_connected_partners=True,
                include_disconnected_partners=False,
            )

            m = np.isinf(affs)
            inf_partners = partners[m]
            new_chunk_split_partners = inf_partners[
                ~np.in1d(inf_partners, chunk_split_partners)
            ]
            atomic_ids.extend(new_chunk_split_partners)
            chunk_split_partners.extend(new_chunk_split_partners)
        return chunk_split_partners

    def get_all_original_partners(self, atomic_id: np.uint64):
        """ Finds all partners from the unchunked region graph
            Merges split supervoxels over chunk boundaries first (affs == inf)
        :param atomic_id: np.uint64
        :return: dict np.uint64 -> np.uint64
        """
        atomic_ids = [atomic_id]
        partner_dict = {}
        while len(atomic_ids) > 0:
            atomic_id = atomic_ids[0]
            del atomic_ids[0]
            partners, affs, _ = self.get_atomic_partners(
                atomic_id,
                include_connected_partners=True,
                include_disconnected_partners=False,
            )

            m = np.isinf(affs)
            partner_dict[atomic_id] = partners[~m]
            inf_partners = partners[m]
            new_chunk_split_partners = inf_partners[
                ~np.in1d(inf_partners, list(partner_dict.keys()))
            ]
            atomic_ids.extend(new_chunk_split_partners)
        return partner_dict

    def get_atomic_node_partners(
        self, atomic_id: np.uint64, time_stamp: datetime.datetime = get_max_time()
    ) -> Dict:
        """ Reads register partner ids

        :param atomic_id: np.uint64
        :param time_stamp: datetime.datetime
        :return: dict
        """
        col_partner = column_keys.Connectivity.Partner
        col_connected = column_keys.Connectivity.Connected
        columns = [col_partner, col_connected]
        row_dict = self.read_node_id_row(
            atomic_id, columns=columns, end_time=time_stamp, end_time_inclusive=True
        )
        flattened_row_dict = self.flatten_row_dict(row_dict)
        return flattened_row_dict[col_partner][flattened_row_dict[col_connected]]

    def _get_atomic_node_info_core(self, row_dict) -> Dict:
        """ Reads connectivity information for a single node

        :param atomic_id: np.uint64
        :param time_stamp: datetime.datetime
        :return: dict
        """
        flattened_row_dict = self.flatten_row_dict(row_dict)
        all_ids = np.arange(
            len(flattened_row_dict[column_keys.Connectivity.Partner]),
            dtype=column_keys.Connectivity.Partner.basetype,
        )
        disconnected_m = ~np.in1d(
            all_ids, flattened_row_dict[column_keys.Connectivity.Connected]
        )
        flattened_row_dict[column_keys.Connectivity.Disconnected] = all_ids[
            disconnected_m
        ]
        return flattened_row_dict

    def get_atomic_node_info(
        self, atomic_id: np.uint64, time_stamp: datetime.datetime = get_max_time()
    ) -> Dict:
        """ Reads connectivity information for a single node
        :param atomic_id: np.uint64
        :param time_stamp: datetime.datetime
        :return: dict
        """
        columns = [
            column_keys.Connectivity.Connected,
            column_keys.Connectivity.Affinity,
            column_keys.Connectivity.Area,
            column_keys.Connectivity.Partner,
            column_keys.Hierarchy.Parent,
        ]
        row_dict = self.read_node_id_row(
            atomic_id, columns=columns, end_time=time_stamp, end_time_inclusive=True
        )
        return self._get_atomic_node_info_core(row_dict)

    def _get_atomic_partners_core(
        self,
        flattened_row_dict: Dict,
        include_connected_partners=True,
        include_disconnected_partners=False,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """ Extracts the atomic partners and affinities for a given timestamp
        :param flattened_row_dict: dict
        :param include_connected_partners: bool
        :param include_disconnected_partners: bool
        :return: list of np.ndarrays
        """
        columns = []
        if include_connected_partners:
            columns.append(column_keys.Connectivity.Connected)
        if include_disconnected_partners:
            columns.append(column_keys.Connectivity.Disconnected)

        included_ids = []
        for column in columns:
            included_ids.extend(flattened_row_dict[column])
        included_ids = np.array(
            included_ids, dtype=column_keys.Connectivity.Connected.basetype
        )

        areas = flattened_row_dict[column_keys.Connectivity.Area][included_ids]
        affinities = flattened_row_dict[column_keys.Connectivity.Affinity][included_ids]
        partners = flattened_row_dict[column_keys.Connectivity.Partner][included_ids]
        return partners, affinities, areas

    def get_atomic_partners(
        self,
        atomic_id: np.uint64,
        include_connected_partners=True,
        include_disconnected_partners=False,
        time_stamp: Optional[datetime.datetime] = get_max_time(),
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """ Extracts the atomic partners and affinities for a given timestamp
        :param atomic_id: np.uint64
        :param include_connected_partners: bool
        :param include_disconnected_partners: bool
        :param time_stamp: None or datetime
        :return: list of np.ndarrays
        """
        assert include_connected_partners or include_disconnected_partners
        flattened_row_dict = self.get_atomic_node_info(atomic_id, time_stamp=time_stamp)
        return self._get_atomic_partners_core(
            flattened_row_dict,
            include_connected_partners,
            include_disconnected_partners,
        )

    def _retrieve_connectivity(
        self,
        dict_item: Tuple[
            np.uint64, Dict[column_keys._Column, List[bigtable.row_data.Cell]]
        ],
        connected_edges: bool = True,
    ):
        node_id, row = dict_item
        tmp = set()
        for x in chain.from_iterable(
            generation.value
            for generation in row[column_keys.Connectivity.Connected][::-1]
        ):
            tmp.remove(x) if x in tmp else tmp.add(x)

        connected_indices = np.fromiter(tmp, np.uint64)
        if column_keys.Connectivity.Partner in row:
            edges = np.fromiter(
                chain.from_iterable(
                    (node_id, partner_id)
                    for generation in row[column_keys.Connectivity.Partner][::-1]
                    for partner_id in generation.value
                ),
                dtype=basetypes.NODE_ID,
            ).reshape((-1, 2))
            edges = self._connected_or_not(edges, connected_indices, connected_edges)
        else:
            edges = np.empty((0, 2), basetypes.NODE_ID)

        if column_keys.Connectivity.Affinity in row:
            affinities = np.fromiter(
                chain.from_iterable(
                    generation.value
                    for generation in row[column_keys.Connectivity.Affinity][::-1]
                ),
                dtype=basetypes.EDGE_AFFINITY,
            )
            affinities = self._connected_or_not(
                affinities, connected_indices, connected_edges
            )
        else:
            affinities = np.empty(0, basetypes.EDGE_AFFINITY)

        if column_keys.Connectivity.Area in row:
            areas = np.fromiter(
                chain.from_iterable(
                    generation.value
                    for generation in row[column_keys.Connectivity.Area][::-1]
                ),
                dtype=basetypes.EDGE_AREA,
            )
            areas = self._connected_or_not(areas, connected_indices, connected_edges)
        else:
            areas = np.empty(0, basetypes.EDGE_AREA)

        return edges, affinities, areas

    def _connected_or_not(self, array, connected_indices, connected):
        """
        Either filters the first dimension of a numpy array by the passed
        indices or their complement. Used to select edge descriptors for
        those that are either connected or not connected.
        """
        mask = np.zeros((array.shape[0],), dtype=np.bool)
        mask[connected_indices] = True
        if connected:
            return array[mask, ...]
        else:
            return array[~mask, ...]

    def get_subgraph_chunk(
        self,
        node_ids: Iterable[np.uint64],
        make_unique: bool = True,
        connected_edges: bool = True,
        time_stamp: Optional[datetime.datetime] = None,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """ Takes an atomic id and returns the associated agglomeration ids

        :param node_ids: array of np.uint64
        :param make_unique: bool
        :param connected_edges: bool
        :param time_stamp: None or datetime
        :return: edge list
        """
        if time_stamp is None:
            time_stamp = datetime.datetime.utcnow()

        if time_stamp.tzinfo is None:
            time_stamp = pytz.UTC.localize(time_stamp)

        child_ids = self.get_children(node_ids, flatten=True)
        row_dict = self.read_node_id_rows(
            node_ids=child_ids,
            columns=[
                column_keys.Connectivity.Area,
                column_keys.Connectivity.Affinity,
                column_keys.Connectivity.Partner,
                column_keys.Connectivity.Connected,
                column_keys.Connectivity.Disconnected,
            ],
            end_time=time_stamp,
            end_time_inclusive=True,
        )

        tmp_edges, tmp_affinites, tmp_areas = [], [], []
        for row_dict_item in row_dict.items():
            edges, affinities, areas = self._retrieve_connectivity(
                row_dict_item, connected_edges
            )
            tmp_edges.append(edges)
            tmp_affinites.append(affinities)
            tmp_areas.append(areas)

        edges = (
            np.concatenate(tmp_edges)
            if tmp_edges
            else np.empty((0, 2), dtype=basetypes.NODE_ID)
        )
        affinities = (
            np.concatenate(tmp_affinites)
            if tmp_affinites
            else np.empty(0, dtype=basetypes.EDGE_AFFINITY)
        )
        areas = (
            np.concatenate(tmp_areas)
            if tmp_areas
            else np.empty(0, dtype=basetypes.EDGE_AREA)
        )

        # If requested, remove duplicate edges. Every edge is stored in each
        # participating node. Hence, we have many edge pairs that look
        # like [x, y], [y, x]. We solve this by sorting and calling np.unique
        # row-wise
        if make_unique and len(edges) > 0:
            edges, idx = np.unique(np.sort(edges, axis=1), axis=0, return_index=True)
            affinities = affinities[idx]
            areas = areas[idx]
        return edges, affinities, areas

    def add_edges(
        self,
        user_id: str,
        atomic_edges: Sequence[np.uint64],
        affinities: Sequence[np.float32] = None,
        source_coord: Sequence[int] = None,
        sink_coord: Sequence[int] = None,
        n_tries: int = 60,
    ) -> GraphEditOperation.Result:
        """ Adds an edge to the chunkedgraph

            Multi-user safe through locking of the root node

            This function acquires a lock and ensures that it still owns the
            lock before executing the write.

        :param user_id: str
            unique id - do not just make something up, use the same id for the
            same user every time
        :param atomic_edges: list of two uint64s
            have to be from the same two root ids!
        :param affinities: list of np.float32 or None
            will eventually be set to 1 if None
        :param source_coord: list of int (n x 3)
        :param sink_coord: list of int (n x 3)
        :param n_tries: int
        :return: GraphEditOperation.Result
        """
        return MergeOperation(
            self,
            user_id=user_id,
            added_edges=atomic_edges,
            affinities=affinities,
            source_coords=source_coord,
            sink_coords=sink_coord,
        ).execute()

    def remove_edges(
        self,
        user_id: str,
        source_ids: Sequence[np.uint64] = None,
        sink_ids: Sequence[np.uint64] = None,
        source_coords: Sequence[Sequence[int]] = None,
        sink_coords: Sequence[Sequence[int]] = None,
        atomic_edges: Sequence[Tuple[np.uint64, np.uint64]] = None,
        mincut: bool = True,
        bb_offset: Tuple[int, int, int] = (240, 240, 24),
        n_tries: int = 20,
    ) -> GraphEditOperation.Result:
        """ Removes edges - either directly or after applying a mincut

            Multi-user safe through locking of the root node

            This function acquires a lock and ensures that it still owns the
            lock before executing the write.

        :param user_id: str
            unique id - do not just make something up, use the same id for the
            same user every time
        :param source_ids: uint64
        :param sink_ids: uint64
        :param atomic_edges: list of 2 uint64
        :param source_coords: list of 3 ints
            [x, y, z] coordinate of source supervoxel
        :param sink_coords: list of 3 ints
            [x, y, z] coordinate of sink supervoxel
        :param mincut:
        :param bb_offset: list of 3 ints
            [x, y, z] bounding box padding beyond box spanned by coordinates
        :param n_tries: int
        :return: GraphEditOperation.Result
        """
        if mincut:
            return MulticutOperation(
                self,
                user_id=user_id,
                source_ids=source_ids,
                sink_ids=sink_ids,
                source_coords=source_coords,
                sink_coords=sink_coords,
                bbox_offset=bb_offset,
            ).execute()

        if not atomic_edges:
            # Shim - can remove this check once all functions call the split properly/directly
            source_ids = [source_ids] if np.isscalar(source_ids) else source_ids
            sink_ids = [sink_ids] if np.isscalar(sink_ids) else sink_ids
            if len(source_ids) != len(sink_ids):
                raise cg_exceptions.PreconditionError(
                    "Split operation require the same number of source and sink IDs"
                )
            atomic_edges = np.array([source_ids, sink_ids]).transpose()
        return SplitOperation(
            self,
            user_id=user_id,
            removed_edges=atomic_edges,
            source_coords=source_coords,
            sink_coords=sink_coords,
        ).execute()

    def undo_operation(
        self, user_id: str, operation_id: np.uint64
    ) -> GraphEditOperation.Result:
        """ Applies the inverse of a previous GraphEditOperation

        :param user_id: str
        :param operation_id: operation_id to be inverted
        :return: GraphEditOperation.Result
        """
        return UndoOperation(self, user_id=user_id, operation_id=operation_id).execute()

    def redo_operation(
        self, user_id: str, operation_id: np.uint64
    ) -> GraphEditOperation.Result:
        """ Re-applies a previous GraphEditOperation

        :param user_id: str
        :param operation_id: operation_id to be repeated
        :return: GraphEditOperation.Result
        """
        return RedoOperation(self, user_id=user_id, operation_id=operation_id).execute()

    def _run_multicut(
        self,
        source_ids: Sequence[np.uint64],
        sink_ids: Sequence[np.uint64],
        source_coords: Sequence[Sequence[int]],
        sink_coords: Sequence[Sequence[int]],
        bb_offset: Tuple[int, int, int] = (120, 120, 12),
    ):
        time_start = time.time()
        bb_offset = np.array(list(bb_offset))
        source_coords = np.array(source_coords)
        sink_coords = np.array(sink_coords)

        # Decide a reasonable bounding box (NOT guaranteed to be successful!)
        coords = np.concatenate([source_coords, sink_coords])
        bounding_box = [np.min(coords, axis=0), np.max(coords, axis=0)]

        bounding_box[0] -= bb_offset
        bounding_box[1] += bb_offset

        # Verify that sink and source are from the same root object
        root_ids = set()
        for source_id in source_ids:
            root_ids.add(self.get_root(source_id))
        for sink_id in sink_ids:
            root_ids.add(self.get_root(sink_id))

        if len(root_ids) > 1:
            raise cg_exceptions.PreconditionError(
                f"All supervoxel must belong to the same object. Already split?"
            )

        self.logger.debug(
            "Get roots and check: %.3fms" % ((time.time() - time_start) * 1000)
        )
        time_start = time.time()  # ------------------------------------------

        root_id = root_ids.pop()

        # Get edges between local supervoxels
        n_chunks_affected = np.product(
            (np.ceil(bounding_box[1] / self.chunk_size)).astype(np.int)
            - (np.floor(bounding_box[0] / self.chunk_size)).astype(np.int)
        )

        self.logger.debug("Number of affected chunks: %d" % n_chunks_affected)
        self.logger.debug(f"Bounding box: {bounding_box}")
        self.logger.debug(f"Bounding box padding: {bb_offset}")
        self.logger.debug(f"Source ids: {source_ids}")
        self.logger.debug(f"Sink ids: {sink_ids}")
        self.logger.debug(f"Root id: {root_id}")

        edges, affs, _ = self.get_subgraph_edges(
            root_id, bounding_box=bounding_box, bb_is_coordinate=True
        )
        self.logger.debug(
            f"Get edges and affs: " f"{(time.time() - time_start) * 1000:.3f}ms"
        )

        time_start = time.time()  # ------------------------------------------

        if len(edges) == 0:
            raise cg_exceptions.PreconditionError(
                f"No local edges found. " f"Something went wrong with the bounding box?"
            )

        # Compute mincut
        atomic_edges = cutting.mincut(edges, affs, source_ids, sink_ids)
        self.logger.debug(f"Mincut: {(time.time() - time_start) * 1000:.3f}ms")
        if len(atomic_edges) == 0:
            raise cg_exceptions.PostconditionError(f"Mincut failed. Try again...")

        # # Check if any edge in the cutset is infinite (== between chunks)
        # # We would prevent such a cut
        #
        # atomic_edges_flattened_view = atomic_edges.view(dtype='u8,u8')
        # edges_flattened_view = edges.view(dtype='u8,u8')
        #
        # cutset_mask = np.in1d(edges_flattened_view, atomic_edges_flattened_view)
        #
        # if np.any(np.isinf(affs[cutset_mask])):
        #     self.logger.error("inf in cutset")
        #     return False, None
        return atomic_edges
