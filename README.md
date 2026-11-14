# partition-compaction-planner
Python 数据湖分区合并规划库，处理小文件分组、大小预算、删除标记和幂等重写

## 定位

只根据输入的数据文件清单**生成合并计划**，不实际移动文件，也不提供文件管理器界面。

## 输入

- `FileEntry`：数据集、分区、文件 ID、行数、字节大小、最小/最大事件时间、是否受管、可选版本号。
- `DeleteMarker`：已存在的删除标记。
- `Constraints`：单个输出文件的最大字节数 `max_output_bytes`、最小行数 `min_output_rows`。

## 合并约束

- 按 `(dataset, partition)` 分组独立规划，绝不跨分区、跨数据集合并。
- `managed=False` 的未受管文件保持原样，不参与任何合并。
- 同一组待合并文件按事件范围检查是否可安全排序：范围重叠且没有全员明确的
  `version` 顺序时，宁可拆成多个结果，也不把先后关系弄乱；事件范围未知
  （`None`）按与任何范围都可能重叠处理。
- 超过 `max_output_bytes` 的超大文件保持原样（`oversized`）。
- 合并结果行数低于 `min_output_rows` 时，在字节预算允许时与相邻组合并，
  否则保持原样（`undersized_group`）。
- 删除标记与新文件生成一一对应：每个 `PlannedOutput` 的源文件都产生对应的
  `PlannedDelete`，可逐条验证。
- 已有删除标记（pending delete）的文件不再参与合并；指向清单中已消失文件的
  标记记入 `stale_markers`，不会重复上报。
- 计划是确定性的：同一份清单重复执行得到完全相同的结果，与输入顺序无关。

## 用法

```python
from compaction_planner import Constraints, FileEntry, plan_compaction

plan = plan_compaction(
    files=[FileEntry(dataset="ds", partition="dt=2026-11-14/hr=00",
                     file_id="f1", rows=100, bytes=4096,
                     event_min=0, event_max=59)],
    constraints=Constraints(max_output_bytes=128 * 1024 * 1024,
                            min_output_rows=1000),
)
for out in plan.outputs:
    print(out.output_id, out.source_file_ids, out.total_bytes)
```

## 测试

```bash
python -m unittest
```
