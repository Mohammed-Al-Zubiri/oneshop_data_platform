"""
OneShop Data Quality Gate — Great Expectations validation for the Bronze layer.

Validates bronze.users and bronze.purchases against their expectation suites
before any Silver transformation runs. Renders HTML Data Docs to MinIO on
every run (pass or fail) so results are always visible.

Exit codes:
  0 — all suites passed
  1 — one or more suites failed (or a fatal runtime error occurred)
"""

import sys
import great_expectations as gx
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# 1. Spark Session
# ---------------------------------------------------------------------------
try:
    spark = SparkSession.builder.appName("gx-bronze-data-quality-gate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
except Exception as exc:
    print(f"[FATAL] Could not create SparkSession: {exc}", file=sys.stderr)
    sys.exit(1)

print("[GX] SparkSession ready. Loading Bronze tables…")

try:
    bronze_users = spark.table("bronze.users")
    bronze_purchases = spark.table("bronze.purchases")
    bronze_items = spark.table("bronze.items")
    bronze_pageviews = spark.table("bronze.pageviews")
except Exception as exc:
    print(f"[FATAL] Could not load Bronze tables: {exc}", file=sys.stderr)
    spark.stop()
    sys.exit(1)

print(f"[GX]   bronze.users     → {bronze_users.count():,} rows")
print(f"[GX]   bronze.purchases → {bronze_purchases.count():,} rows")
print(f"[GX]   bronze.items     → {bronze_items.count():,} rows")
print(f"[GX]   bronze.pageviews → {bronze_pageviews.count():,} rows")

# ---------------------------------------------------------------------------
# 2. Great Expectations FileDataContext
# ---------------------------------------------------------------------------
GX_ROOT = "/home/iceberg/great-expectations"

try:
    context = gx.get_context(context_root_dir=GX_ROOT)
except Exception as exc:
    print(f"[FATAL] Could not load GX context from '{GX_ROOT}': {exc}", file=sys.stderr)
    spark.stop()
    sys.exit(1)

print(f"[GX] FileDataContext loaded from {GX_ROOT}")


# ---------------------------------------------------------------------------
# 3. Helper — run checkpoint
# ---------------------------------------------------------------------------
def execute_checkpoint(df, *, checkpoint_name: str, asset_name: str):
    """
    Run *checkpoint_name* against *df* by overriding runtime parameters.
    Returns a CheckpointResult object.
    """
    return context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        batch_request={
            "runtime_parameters": {"batch_data": df},
            "batch_identifiers": {"default_identifier_name": asset_name},
        },
    )


# ---------------------------------------------------------------------------
# 4. Run validation checkpoints
# ---------------------------------------------------------------------------
print("\n[GX] ── Running validation checkpoints ─────────────────────────")

print("[GX] Validating bronze.users with 'bronze_users_checkpoint' …")
users_result = execute_checkpoint(
    bronze_users,
    checkpoint_name="bronze_users_checkpoint",
    asset_name="bronze_users",
)

print("[GX] Validating bronze.purchases with 'bronze_purchases_checkpoint' …")
purchases_result = execute_checkpoint(
    bronze_purchases,
    checkpoint_name="bronze_purchases_checkpoint",
    asset_name="bronze_purchases",
)

print("[GX] Validating bronze.items with 'bronze_items_checkpoint' …")
items_result = execute_checkpoint(
    bronze_items,
    checkpoint_name="bronze_items_checkpoint",
    asset_name="bronze_items",
)

print("[GX] Validating bronze.pageviews with 'bronze_pageviews_checkpoint' …")
pageviews_result = execute_checkpoint(
    bronze_pageviews,
    checkpoint_name="bronze_pageviews_checkpoint",
    asset_name="bronze_pageviews",
)

all_passed = (
    users_result.success
    and purchases_result.success
    and items_result.success
    and pageviews_result.success
)

# ---------------------------------------------------------------------------
# 5. Render Data Docs → MinIO (always, even on failure)
# ---------------------------------------------------------------------------
print("\n[GX] ── Rendering Data Docs to MinIO ──────────────────────")
try:
    context.build_data_docs(site_names=["minio_site"])
    print("[GX] Data Docs published → s3://warehouse/data_docs/")
except Exception as exc:
    # Non-fatal: docs rendering failure must not mask a validation failure
    print(f"[WARNING] Data Docs rendering failed: {exc}", file=sys.stderr)

# ---------------------------------------------------------------------------
# 6. Print summary and exit
# ---------------------------------------------------------------------------
users_badge = "✅ PASSED" if users_result.success else "❌ FAILED"
purchases_badge = "✅ PASSED" if purchases_result.success else "❌ FAILED"
items_badge = "✅ PASSED" if items_result.success else "❌ FAILED"
pageviews_badge = "✅ PASSED" if pageviews_result.success else "❌ FAILED"

print("\n[GX] ── Validation Summary ─────────────────────────────────")
print(f"[GX]   bronze.users     checkpoint → {users_badge}")
if not users_result.success:
    for val_result in users_result.list_validation_results():
        if not val_result.success:
            for r in val_result.results:
                if not r.success:
                    print(
                        f"[GX]     • {r.expectation_config.expectation_type}"
                        f"  kwargs={r.expectation_config.kwargs}"
                    )

print(f"[GX]   bronze.purchases checkpoint → {purchases_badge}")
if not purchases_result.success:
    for val_result in purchases_result.list_validation_results():
        if not val_result.success:
            for r in val_result.results:
                if not r.success:
                    print(
                        f"[GX]     • {r.expectation_config.expectation_type}"
                        f"  kwargs={r.expectation_config.kwargs}"
                    )

print(f"[GX]   bronze.items     checkpoint → {items_badge}")
if not items_result.success:
    for val_result in items_result.list_validation_results():
        if not val_result.success:
            for r in val_result.results:
                if not r.success:
                    print(
                        f"[GX]     • {r.expectation_config.expectation_type}"
                        f"  kwargs={r.expectation_config.kwargs}"
                    )

print(f"[GX]   bronze.pageviews checkpoint → {pageviews_badge}")
if not pageviews_result.success:
    for val_result in pageviews_result.list_validation_results():
        if not val_result.success:
            for r in val_result.results:
                if not r.success:
                    print(
                        f"[GX]     • {r.expectation_config.expectation_type}"
                        f"  kwargs={r.expectation_config.kwargs}"
                    )

print("[GX] ────────────────────────────────────────────────────────\n")

spark.stop()

if not all_passed:
    print(
        "[GATE] ❌ One or more validation suites FAILED. "
        "Halting pipeline — Silver transform will NOT run.",
        file=sys.stderr,
    )
    sys.exit(1)

print("[GATE] ✅ All validation suites passed. Proceeding to Silver transform.")
sys.exit(0)
