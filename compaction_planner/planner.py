"""合并计划生成器。

只根据输入清单计算合并计划，不读写任何真实文件。
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional, Sequence, Tuple

from .models import (
    CompactionPlan,
    Constraints,
    DeleteMarker,
    FileEntry,
    PlannedDelete,
    PlannedOutput,
    SkippedFile,
)

_NEG_INF = float("-inf")
_POS_INF = float("inf")


def plan_compaction(
    files: Sequence[FileEntry],
    constraints: Constraints,
    delete_markers: Sequence[DeleteMarker] = (),
) -> CompactionPlan:
    """根据文件清单生成确定性的合并计划。

    规则：
    - 按 (dataset, partition) 分组处理，绝不跨分区合并；
    - managed=False 的文件保持原样；
    - 已有删除标记（pending delete）的文件不再参与合并；
    - 同一组内按事件范围检查可安全排序性：范围重叠且没有
      全员明确的 version 顺序时，拆成多个独立结果；
    - 超过 max_output_bytes 的单文件保持原样；
    - 合并结果行数低于 min_output_rows 时，在字节预算允许时
      与相邻组合并，否则保持原样；
    - 指向清单中已消失文件的删除标记记入 stale_markers，不重复上报。
    """
    if constraints.max_output_bytes <= 0:
        raise ValueError("max_output_bytes 必须为正数")
    if constraints.min_output_rows < 0:
        raise ValueError("min_output_rows 不能为负数")

    entries = _validate_and_sort_files(files)
    known = {(f.dataset, f.partition, f.file_id) for f in entries}
    pending_delete, stale = _classify_markers(delete_markers, known)

    skipped: List[SkippedFile] = []
    groups: Dict[Tuple[str, str], List[FileEntry]] = {}
    for entry in entries:
        if not entry.managed:
            skipped.append(_skip(entry, "unmanaged"))
        elif (entry.dataset, entry.partition, entry.file_id) in pending_delete:
            skipped.append(_skip(entry, "pending_delete"))
        else:
            groups.setdefault(entry.key, []).append(entry)

    outputs: List[PlannedOutput] = []
    for key in sorted(groups):
        group_outputs, group_skipped = _plan_group(groups[key], constraints)
        outputs.extend(group_outputs)
        skipped.extend(group_skipped)

    deletes = tuple(
        PlannedDelete(
            dataset=out.dataset,
            partition=out.partition,
            file_id=src,
            output_id=out.output_id,
        )
        for out in outputs
        for src in out.source_file_ids
    )
    return CompactionPlan(
        outputs=tuple(outputs),
        deletes=deletes,
        skipped=tuple(sorted(skipped, key=_skip_sort_key)),
        stale_markers=tuple(stale),
    )


def _validate_and_sort_files(files: Sequence[FileEntry]) -> List[FileEntry]:
    seen = set()
    for entry in files:
        ident = (entry.dataset, entry.partition, entry.file_id)
        if ident in seen:
            raise ValueError(f"清单中存在重复文件: {ident}")
        seen.add(ident)
        if entry.rows < 0 or entry.bytes < 0:
            raise ValueError(f"行数或字节数不能为负: {ident}")
    return sorted(files, key=lambda f: (f.dataset, f.partition, f.file_id))


def _classify_markers(
    markers: Sequence[DeleteMarker],
    known: set,
) -> Tuple[set, List[DeleteMarker]]:
    pending = set()
    stale_map = {}
    for marker in markers:
        ident = (marker.dataset, marker.partition, marker.file_id)
        if ident in known:
            pending.add(ident)
        else:
            stale_map[ident] = marker
    stale = [stale_map[k] for k in sorted(stale_map)]
    return pending, stale


def _plan_group(
    group: List[FileEntry],
    constraints: Constraints,
) -> Tuple[List[PlannedOutput], List[SkippedFile]]:
    skipped: List[SkippedFile] = []
    candidates: List[FileEntry] = []
    for entry in group:
        if entry.bytes > constraints.max_output_bytes:
            skipped.append(_skip(entry, "oversized"))
        else:
            candidates.append(entry)

    outputs: List[PlannedOutput] = []
    for cluster in _order_safe_clusters(candidates):
        for packed in _pack_cluster(cluster, constraints):
            if len(packed) < 2:
                reason = (
                    "undersized_group"
                    if packed[0].rows < constraints.min_output_rows
                    else "singleton"
                )
                skipped.append(_skip(packed[0], reason))
            elif _group_rows(packed) < constraints.min_output_rows:
                skipped.extend(_skip(f, "undersized_group") for f in packed)
            else:
                outputs.append(_make_output(packed))
    outputs.sort(key=lambda o: o.output_id)
    return outputs, skipped


def _order_safe_clusters(candidates: List[FileEntry]) -> List[List[FileEntry]]:
    """把候选文件拆成可安全排序的簇。

    全员都有 version 时按版本排序，整体为一个簇；否则按事件范围
    贪心分簇：范围与当前簇重叠的文件另起新簇，绝不混入同一结果。
    """
    if not candidates:
        return []
    if all(f.version is not None for f in candidates):
        return [sorted(candidates, key=lambda f: (f.version, f.file_id))]

    ordered = sorted(candidates, key=lambda f: (_lo(f), _hi(f), f.file_id))
    clusters: List[List[FileEntry]] = []
    cluster_hi: List[float] = []
    for entry in ordered:
        if clusters and _lo(entry) > cluster_hi[-1]:
            clusters[-1].append(entry)
            cluster_hi[-1] = max(cluster_hi[-1], _hi(entry))
        else:
            clusters.append([entry])
            cluster_hi.append(_hi(entry))
    return clusters


def _pack_cluster(
    cluster: List[FileEntry],
    constraints: Constraints,
) -> List[List[FileEntry]]:
    """在保持顺序的前提下按字节预算贪心装箱，再兜底最小行数。"""
    groups: List[List[FileEntry]] = []
    current: List[FileEntry] = []
    current_bytes = 0
    for entry in cluster:
        if current and current_bytes + entry.bytes > constraints.max_output_bytes:
            groups.append(current)
            current = []
            current_bytes = 0
        current.append(entry)
        current_bytes += entry.bytes
    if current:
        groups.append(current)

    merged: List[List[FileEntry]] = []
    for group in groups:
        if (
            merged
            and _group_rows(merged[-1]) < constraints.min_output_rows
            and _group_bytes(merged[-1]) + _group_bytes(group)
            <= constraints.max_output_bytes
        ):
            merged[-1] = merged[-1] + group
        else:
            merged.append(group)
    return merged


def _make_output(packed: List[FileEntry]) -> PlannedOutput:
    source_ids = tuple(f.file_id for f in packed)
    digest = hashlib.sha1(
        "|".join(
            [packed[0].dataset, packed[0].partition, ",".join(source_ids)]
        ).encode("utf-8")
    ).hexdigest()[:16]
    mins = [f.event_min for f in packed]
    maxs = [f.event_max for f in packed]
    return PlannedOutput(
        dataset=packed[0].dataset,
        partition=packed[0].partition,
        output_id=f"out-{digest}",
        source_file_ids=source_ids,
        total_rows=_group_rows(packed),
        total_bytes=_group_bytes(packed),
        event_min=min(mins) if all(v is not None for v in mins) else None,
        event_max=max(maxs) if all(v is not None for v in maxs) else None,
    )


def _lo(entry: FileEntry) -> float:
    return _NEG_INF if entry.event_min is None else float(entry.event_min)


def _hi(entry: FileEntry) -> float:
    return _POS_INF if entry.event_max is None else float(entry.event_max)


def _group_rows(group: Sequence[FileEntry]) -> int:
    return sum(f.rows for f in group)


def _group_bytes(group: Sequence[FileEntry]) -> int:
    return sum(f.bytes for f in group)


def _skip(entry: FileEntry, reason: str) -> SkippedFile:
    return SkippedFile(
        dataset=entry.dataset,
        partition=entry.partition,
        file_id=entry.file_id,
        reason=reason,
    )


def _skip_sort_key(skip: SkippedFile) -> Tuple[str, str, str]:
    return (skip.dataset, skip.partition, skip.file_id)
