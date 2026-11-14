import unittest

from compaction_planner import (
    Constraints,
    DeleteMarker,
    FileEntry,
    plan_compaction,
)

CONSTRAINTS = Constraints(max_output_bytes=1000, min_output_rows=10)


def make_file(file_id, rows=10, size=100, lo=0, hi=9, dataset="ds",
              partition="p1", managed=True, version=None):
    return FileEntry(
        dataset=dataset,
        partition=partition,
        file_id=file_id,
        rows=rows,
        bytes=size,
        event_min=lo,
        event_max=hi,
        managed=managed,
        version=version,
    )


def fragmented(count, dataset="ds", partition="p1", rows=10, size=100):
    # 每个文件占一段互不重叠的事件范围，可安全合并。
    return [
        make_file(
            f"{partition}-f{i:03d}",
            rows=rows,
            size=size,
            lo=i * 10,
            hi=i * 10 + 9,
            dataset=dataset,
            partition=partition,
        )
        for i in range(count)
    ]


class FragmentationTest(unittest.TestCase):
    def test_thousands_of_tiny_files_merge_into_full_outputs(self):
        files = fragmented(2500, rows=4, size=40)
        plan = plan_compaction(files, CONSTRAINTS)
        self.assertTrue(plan.outputs)
        for out in plan.outputs:
            self.assertLessEqual(out.total_bytes, 1000)
            self.assertGreaterEqual(out.total_rows, 10)
            self.assertGreaterEqual(len(out.source_file_ids), 2)
        merged = {src for out in plan.outputs for src in out.source_file_ids}
        self.assertEqual(len(merged), sum(len(o.source_file_ids) for o in plan.outputs))
        # 除无法凑满最小行数的尾部文件外，其余都应被合并。
        self.assertEqual(
            len(files),
            len(merged) + sum(1 for s in plan.skipped if s.reason == "undersized_group"),
        )

    def test_output_preserves_event_order(self):
        files = fragmented(50)
        plan = plan_compaction(files, CONSTRAINTS)
        by_id = {f.file_id: f for f in files}
        for out in plan.outputs:
            ranges = [(by_id[s].event_min, by_id[s].event_max) for s in out.source_file_ids]
            self.assertEqual(ranges, sorted(ranges))
            for prev, nxt in zip(ranges, ranges[1:]):
                self.assertLess(prev[1], nxt[0])


class OversizedFileTest(unittest.TestCase):
    def test_oversized_file_stays_as_is(self):
        big = make_file("big", rows=10**6, size=5000, lo=0, hi=5)
        small = fragmented(5, rows=10, size=100)
        for i, f in enumerate(small):
            object.__setattr__(f, "event_min", 10 + i * 10)
            object.__setattr__(f, "event_max", 19 + i * 10)
        plan = plan_compaction([big] + small, CONSTRAINTS)
        skipped = {s.file_id: s.reason for s in plan.skipped}
        self.assertEqual(skipped["big"], "oversized")
        merged = {src for out in plan.outputs for src in out.source_file_ids}
        self.assertNotIn("big", merged)
        self.assertEqual(len(plan.outputs), 1)


class PartitionIsolationTest(unittest.TestCase):
    def test_never_merges_across_partitions_or_datasets(self):
        files = (
            fragmented(5, dataset="ds", partition="p1")
            + fragmented(5, dataset="ds", partition="p2")
            + fragmented(5, dataset="other", partition="p1")
        )
        plan = plan_compaction(files, CONSTRAINTS)
        self.assertEqual(len(plan.outputs), 3)
        for out in plan.outputs:
            for src in out.source_file_ids:
                self.assertTrue(src.startswith(out.partition))
        partitions = {(o.dataset, o.partition) for o in plan.outputs}
        self.assertEqual(
            partitions, {("ds", "p1"), ("ds", "p2"), ("other", "p1")}
        )


class EventRangeOverlapTest(unittest.TestCase):
    def test_overlapping_ranges_split_into_separate_outputs(self):
        files = [
            make_file("a", lo=0, hi=100),
            make_file("b", lo=50, hi=150),
            make_file("c", lo=200, hi=300),
            make_file("d", lo=400, hi=500),
        ]
        plan = plan_compaction(files, CONSTRAINTS)
        outputs_by_source = {}
        for out in plan.outputs:
            for src in out.source_file_ids:
                outputs_by_source[src] = out.output_id
        # a 与 b 范围重叠，绝不能出现在同一个输出里。
        if "a" in outputs_by_source and "b" in outputs_by_source:
            self.assertNotEqual(outputs_by_source["a"], outputs_by_source["b"])
        else:
            self.assertNotEqual(
                {"a", "b"} - set(outputs_by_source), set(),
                "重叠文件应拆分或保持原样，而不是合并在一起",
            )
        # c 与 d 互不重叠，可以安全合并。
        self.assertIn("c", outputs_by_source)
        self.assertEqual(outputs_by_source.get("c"), outputs_by_source.get("d"))

    def test_explicit_version_order_allows_overlap_merge(self):
        files = [
            make_file("v2", lo=0, hi=100, version=2),
            make_file("v1", lo=40, hi=90, version=1),
            make_file("v3", lo=60, hi=120, version=3),
        ]
        plan = plan_compaction(files, CONSTRAINTS)
        self.assertEqual(len(plan.outputs), 1)
        self.assertEqual(plan.outputs[0].source_file_ids, ("v1", "v2", "v3"))

    def test_unknown_range_treated_as_overlapping(self):
        files = [
            make_file("known", lo=0, hi=10),
            make_file("unknown", lo=None, hi=None),
        ]
        plan = plan_compaction(files, CONSTRAINTS)
        merged = [set(o.source_file_ids) for o in plan.outputs]
        self.assertNotIn({"known", "unknown"}, merged)


class UnmanagedFileTest(unittest.TestCase):
    def test_unmanaged_files_are_never_touched(self):
        files = fragmented(5) + [
            make_file("ext1", managed=False, lo=500, hi=509),
            make_file("ext2", managed=False, lo=510, hi=519),
        ]
        plan = plan_compaction(files, CONSTRAINTS)
        skipped = {s.file_id: s.reason for s in plan.skipped}
        self.assertEqual(skipped["ext1"], "unmanaged")
        self.assertEqual(skipped["ext2"], "unmanaged")
        merged = {src for out in plan.outputs for src in out.source_file_ids}
        self.assertNotIn("ext1", merged)
        self.assertNotIn("ext2", merged)
        self.assertEqual(len(plan.outputs), 1)


class DeleteMarkerTest(unittest.TestCase):
    def test_deletes_match_new_outputs_verifiably(self):
        files = fragmented(20)
        plan = plan_compaction(files, CONSTRAINTS)
        by_output = {}
        for delete in plan.deletes:
            by_output.setdefault(delete.output_id, set()).add(delete.file_id)
        self.assertEqual(len(by_output), len(plan.outputs))
        for out in plan.outputs:
            self.assertEqual(by_output[out.output_id], set(out.source_file_ids))
            for delete in plan.deletes:
                if delete.output_id == out.output_id:
                    self.assertEqual(delete.dataset, out.dataset)
                    self.assertEqual(delete.partition, out.partition)

    def test_pending_delete_files_are_not_merged(self):
        files = fragmented(6)
        markers = [DeleteMarker("ds", "p1", "p1-f000")]
        plan = plan_compaction(files, CONSTRAINTS, delete_markers=markers)
        skipped = {s.file_id: s.reason for s in plan.skipped}
        self.assertEqual(skipped["p1-f000"], "pending_delete")
        merged = {src for out in plan.outputs for src in out.source_file_ids}
        self.assertNotIn("p1-f000", merged)

    def test_stale_markers_not_re_reported(self):
        files = fragmented(5)
        markers = [
            DeleteMarker("ds", "p1", "ghost-file"),
            DeleteMarker("ds", "p9", "ghost-file"),
        ]
        plan = plan_compaction(files, CONSTRAINTS, delete_markers=markers)
        self.assertEqual(
            {(m.dataset, m.partition, m.file_id) for m in plan.stale_markers},
            {("ds", "p1", "ghost-file"), ("ds", "p9", "ghost-file")},
        )
        deleted_ids = {d.file_id for d in plan.deletes}
        self.assertNotIn("ghost-file", deleted_ids)


class EmptyManifestTest(unittest.TestCase):
    def test_empty_manifest_gives_empty_plan(self):
        plan = plan_compaction([], CONSTRAINTS)
        self.assertEqual(plan.outputs, ())
        self.assertEqual(plan.deletes, ())
        self.assertEqual(plan.skipped, ())
        self.assertEqual(plan.stale_markers, ())

    def test_only_stale_markers_with_empty_manifest(self):
        markers = [DeleteMarker("ds", "p1", "gone")]
        plan = plan_compaction([], CONSTRAINTS, delete_markers=markers)
        self.assertEqual(plan.outputs, ())
        self.assertEqual(len(plan.stale_markers), 1)


class DeterminismTest(unittest.TestCase):
    def test_same_manifest_same_plan(self):
        files = fragmented(300, rows=7, size=33)
        markers = [DeleteMarker("ds", "p1", "ghost")]
        first = plan_compaction(files, CONSTRAINTS, delete_markers=markers)
        second = plan_compaction(files, CONSTRAINTS, delete_markers=markers)
        self.assertEqual(first, second)

    def test_input_order_does_not_change_plan(self):
        files = fragmented(120, rows=9, size=61)
        shuffled = list(reversed(files))
        self.assertEqual(
            plan_compaction(files, CONSTRAINTS),
            plan_compaction(shuffled, CONSTRAINTS),
        )

    def test_rerun_after_applying_plan_has_no_leftover_work(self):
        files = fragmented(40)
        first = plan_compaction(files, CONSTRAINTS)
        self.assertTrue(first.outputs)
        deleted = {d.file_id for d in first.deletes}
        remaining = [f for f in files if f.file_id not in deleted]
        second = plan_compaction(remaining, CONSTRAINTS)
        # 已删除的文件不再出现，剩余文件无可合并内容。
        self.assertEqual(second.outputs, ())
        self.assertEqual(second.deletes, ())


class MinRowsBudgetTest(unittest.TestCase):
    def test_undersized_tail_merges_forward_when_budget_allows(self):
        files = [
            make_file("a", rows=4, size=100, lo=0, hi=9),
            make_file("b", rows=4, size=100, lo=10, hi=19),
            make_file("c", rows=4, size=100, lo=20, hi=29),
        ]
        plan = plan_compaction(files, CONSTRAINTS)
        self.assertEqual(len(plan.outputs), 1)
        self.assertEqual(plan.outputs[0].total_rows, 12)

    def test_undersized_group_kept_as_is_when_budget_exceeded(self):
        files = [
            make_file("a", rows=60, size=950, lo=0, hi=9),
            make_file("b", rows=1, size=100, lo=10, hi=19),
        ]
        plan = plan_compaction(files, CONSTRAINTS)
        self.assertEqual(plan.outputs, ())
        skipped = {s.file_id: s.reason for s in plan.skipped}
        self.assertEqual(skipped["b"], "undersized_group")


class ValidationTest(unittest.TestCase):
    def test_duplicate_file_ids_rejected(self):
        files = [make_file("dup"), make_file("dup")]
        with self.assertRaises(ValueError):
            plan_compaction(files, CONSTRAINTS)

    def test_invalid_constraints_rejected(self):
        with self.assertRaises(ValueError):
            plan_compaction([], Constraints(max_output_bytes=0, min_output_rows=1))
        with self.assertRaises(ValueError):
            plan_compaction([], Constraints(max_output_bytes=10, min_output_rows=-1))


if __name__ == "__main__":
    unittest.main()
