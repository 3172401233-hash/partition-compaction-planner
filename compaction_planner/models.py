"""合并规划的数据模型。

所有模型均为不可变 dataclass，保证同一份输入清单重复规划时
得到完全一致的结果（计划确定性）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple


@dataclass(frozen=True)
class FileEntry:
    """清单中的一个数据文件。

    event_min/event_max 为文件覆盖的事件时间范围（闭区间），
    允许为 None 表示未知；未知范围按“与任何范围都可能重叠”处理。
    version 非空时表示存在明确的版本顺序，范围重叠时仍可安全排序。
    """

    dataset: str
    partition: str
    file_id: str
    rows: int
    bytes: int
    event_min: Optional[int] = None
    event_max: Optional[int] = None
    managed: bool = True
    version: Optional[int] = None

    @property
    def key(self) -> Tuple[str, str]:
        return (self.dataset, self.partition)


@dataclass(frozen=True)
class DeleteMarker:
    """输入清单附带的删除标记，指向某个数据文件。"""

    dataset: str
    partition: str
    file_id: str


@dataclass(frozen=True)
class Constraints:
    """合并约束：单个输出文件的最大字节数与最小行数。"""

    max_output_bytes: int
    min_output_rows: int


@dataclass(frozen=True)
class PlannedOutput:
    """一个计划生成的输出文件及其来源映射。"""

    dataset: str
    partition: str
    output_id: str
    source_file_ids: Tuple[str, ...]
    total_rows: int
    total_bytes: int
    event_min: Optional[int]
    event_max: Optional[int]


@dataclass(frozen=True)
class PlannedDelete:
    """计划删除的源文件，与某个输出文件形成可验证的对应关系。"""

    dataset: str
    partition: str
    file_id: str
    output_id: str


@dataclass(frozen=True)
class SkippedFile:
    """保持原样、不参与合并的文件及原因。"""

    dataset: str
    partition: str
    file_id: str
    reason: str  # "unmanaged" | "oversized" | "undersized_group"


@dataclass(frozen=True)
class CompactionPlan:
    """一次规划的结果。

    outputs: 计划新生成的文件（含来源映射）。
    deletes: 计划删除的源文件，每条都关联到某个 output_id。
    skipped: 保持原样的文件及原因。
    stale_markers: 指向清单中已消失文件的删除标记，不会重复上报。
    """

    outputs: Tuple[PlannedOutput, ...] = field(default_factory=tuple)
    deletes: Tuple[PlannedDelete, ...] = field(default_factory=tuple)
    skipped: Tuple[SkippedFile, ...] = field(default_factory=tuple)
    stale_markers: Tuple[DeleteMarker, ...] = field(default_factory=tuple)
