"""时间旅行领域服务：按快照/时间点回溯查询历史湖表数据。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class TimeTravelService(BaseModuleService):
    MODULE = "time_travel"
    ENTITY = "回溯查询"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["查询编号", "所属湖表", "目标快照", "回溯时间点", "查询列", "返回行数", "命中分区数", "查询耗时"]
    REQUIRED_FIELDS = ["查询编号", "所属湖表", "目标快照"]
    STATUS_ORDER = ["待执行", "执行中", "已返回", "执行失败"]
    ACTION_RULES = {"发起查询": "执行中", "取消查询": "已返回", "标记失败": "执行失败"}
    NEGATIVE_ACTIONS = []

    def read_at_snapshot(self, lake_table: str, snapshot_id: str,
                         columns: list[str] | str | None = None,
                         consumer: str | None = None,
                         authorization_receipt: dict[str, Any] | None = None,
                         **kwargs: Any) -> dict[str, Any]:
        """按指定快照 ID 读取历史数据并返回命中分区数和行数。

        传入 ``consumer`` 或 ``authorization_receipt`` 时，会叠加消费视图的
        分区撤回范围，使时间旅行/快照读取也不能绕过授权撤回。
        """
        # 兼容 read_at_snapshot(table, snap, "consumer-name") 的位置调用。
        if consumer is None and isinstance(columns, str) and kwargs.get("columns") is None:
            consumer = columns
            columns = None
        if consumer is None:
            consumer = kwargs.get("consumer_id") or kwargs.get("downstream")
        if authorization_receipt is None:
            authorization_receipt = kwargs.get("auth_receipt") or kwargs.get("authorization")

        snap = lake_store.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return {"success": False, "error": "snapshot not found", "rows": 0, "partitions": 0}

        revoked_partitions: set[str] = set()
        if authorization_receipt:
            revoked_partitions.update(authorization_receipt.get("revoked_partitions") or [])
        if consumer and not authorization_receipt:
            # 延迟导入避免服务注册表初始化阶段的循环导入。
            from app.services.publish_view import consumption_ledger
            revoked_partitions.update(
                consumption_ledger.revoked_partitions(lake_table, consumer)
            )

        total_rows = 0
        consumable_rows = 0
        partitions: set[str] = set()
        consumable_partitions: set[str] = set()
        file_ids: list[str] = []
        consumable_file_ids: list[str] = []
        blocked_file_ids: list[str] = []
        for fid in snap.file_ids:
            pf = lake_store.get_file(lake_table, fid)
            if pf:
                total_rows += pf.file_rows
                partitions.add(pf.partition_key)
            file_ids.append(fid)
            partition_key = getattr(pf, "partition_key", None) if pf else None
            if partition_key in revoked_partitions:
                blocked_file_ids.append(fid)
                continue
            consumable_file_ids.append(fid)
            if pf:
                consumable_rows += pf.file_rows
                consumable_partitions.add(pf.partition_key)

        authorized_read = bool(consumer or authorization_receipt)
        visible_rows = consumable_rows if authorized_read else total_rows
        visible_partitions = consumable_partitions if authorized_read else partitions
        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "columns": columns,
            "consumer": consumer,
            "rows": visible_rows,
            "total_rows": total_rows,
            "row_count": visible_rows,
            "partitions": len(visible_partitions),
            "partition_count": len(visible_partitions),
            "partitions_hit": sorted(visible_partitions),
            "all_partitions_hit": sorted(partitions),
            "file_ids": consumable_file_ids if authorized_read else file_ids,
            "consumable_file_ids": consumable_file_ids,
            "allowed_file_ids": consumable_file_ids,
            "blocked_file_ids": blocked_file_ids,
            "revoked_file_ids": blocked_file_ids,
            "revoked_partitions": sorted(revoked_partitions),
            "authorization_receipt": authorization_receipt,
            "query_time": time.time(),
        }

    def read_at_timepoint(self, lake_table: str, timestamp: float, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """按时间点回溯查找最近可用快照并读取。"""
        snap = lake_store.get_snapshot_at_time(lake_table, timestamp)
        if snap is None:
            return {"success": False, "error": "no available snapshot at time", "rows": 0, "partitions": 0}
        return self.read_at_snapshot(lake_table, snap.snapshot_id, *args, **kwargs)

    def compare_two_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的数据差异。"""
        s_a = lake_store.get_snapshot(lake_table, snap_a)
        s_b = lake_store.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"success": False, "error": "snapshot not found"}
        return {
            "success": True,
            "snap_a": snap_a,
            "snap_b": snap_b,
            "diff_added_rows": max(0, s_b.added_rows - s_a.added_rows),
            "diff_deleted_rows": max(0, s_b.deleted_rows - s_a.deleted_rows),
        }