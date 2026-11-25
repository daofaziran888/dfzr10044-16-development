from app.lake_store import lake_store
from app.services.publish_view import PublishViewService


def _seed(table: str) -> None:
    for partition, count in (("p1", 1), ("p2", 2)):
        for _ in range(count):
            lake_store.register_file(table, partition, rows=10, bytes_=128)


def test_partition_revocation_fences_old_cursor_and_keeps_other_consumer():
    table = "test_publish_revocation_scenario"
    _seed(table)
    svc = PublishViewService()
    svc.grant_partition_access(table, "jia", ["p1", "p2"])
    svc.grant_partition_access(table, "yi", ["p1", "p2"])

    p1 = svc.fetch_page(table, "jia", partition="p1", page_size=1)
    p2_first = svc.fetch_page(cursor=p1["next_cursor"], partition="p2", page_size=1)
    p2_second = svc.claim_page(cursor=p2_first["next_cursor"], partition="p2", page_size=1)

    revocation = svc.revoke_partition_access(table, "jia", ["p2"], "apply-001")
    duplicate = svc.revoke_partition_access(table, "jia", ["p2"], "apply-001")

    assert revocation["effective"] is True
    assert duplicate["duplicate"] is True
    assert len(svc.list_partition_revocations(table, "jia")) == 1

    denied = svc.fetch_page(cursor=p2_second["cursor"], partition="p2")
    assert denied["success"] is False
    assert denied["authorized"] is False
    assert p2_second["file_ids"][0] in denied["blocked_file_ids"]

    bob = svc.list_consumable_files(table, "yi", "p2")
    assert len(bob) == 2

    other = svc.get_authorization_receipt("other_lake_table", "jia", "p2")
    assert other["partition_authorized"] is False

    confirmation = svc.confirm_delivery(p2_second["claim_id"], delivered=False)
    assert confirmation["confirmed"] is True
    assert confirmation["delivered"] is False
    assert confirmation["confirmation_id"]

    continued = svc.continue_fetch(cursor=p2_second["cursor"])
    assert "p2" in continued["undelivered_partitions"]
    assert "p2" in continued["revoked_undelivered_partitions"]

    recon = svc.submit_reconciliation(
        table,
        "jia",
        [*p1["file_ids"], *p2_first["file_ids"]],
    )
    stats = recon["stats"]
    assert stats["actually_revoked_files"] == p2_first["file_ids"]
    assert stats["first_read_blocked_files"] == p2_second["file_ids"]
