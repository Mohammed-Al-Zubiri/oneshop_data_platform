"""
OneShop — Unified Data Generator CLI
Modes:
  batch     - Seed Postgres OLTP tables + MinIO pageview events (Chapter 3)
  cdc       - Update items inventory for Debezium CDC (Chapter 6)
  purchases - Stream purchase events to Kafka (Chapter 7)
  logins    - Stream login events to Kafka for Flink (Chapter 8)
  reviews   - Seed customer reviews into Postgres (Chapter 10)
"""

import click
from generators.batch import run_batch_seed
from generators.cdc_inventory import run_cdc_inventory
from generators.cdc_purchases import run_cdc_purchases
from generators.login_events import run_login_events
from generators.reviews import run_reviews_seed


@click.group()
def cli():
    """OneShop Data Generator — seed and simulate data for the platform."""
    pass


@cli.command()
@click.option("--users", default=100, help="Number of users to generate")
@click.option("--items", default=1000, help="Number of items to generate")
@click.option("--purchases", default=5000, help="Number of purchases to generate")
@click.option("--pageviews", default=10000, help="Number of pageview events")
def batch(users, items, purchases, pageviews):
    """Seed Postgres OLTP tables and MinIO pageview events (Ch 3)."""
    click.echo(
        f"🚀 Generating batch data: {users} users, {items} items, {purchases} purchases, {pageviews} pageviews"
    )
    run_batch_seed(users, items, purchases, pageviews)
    click.echo("✅ Batch seed complete.")


@cli.command()
@click.option("--count", default=1000, help="Number of inventory updates")
@click.option("--interval", default=1.0, help="Seconds between updates")
def cdc(count, interval):
    """Simulate inventory changes for Debezium CDC (Ch 6)."""
    click.echo(f"🔄 Simulating {count} inventory updates (interval: {interval}s)")
    run_cdc_inventory(count, interval)
    click.echo("✅ CDC inventory updates complete.")


@cli.command()
@click.option("--count", default=500, help="Number of purchase events")
@click.option("--interval", default=0.5, help="Seconds between events")
@click.option("--campaign", default="FLASH2025", help="Campaign ID")
def purchases(count, interval, campaign):
    """Stream flash sale purchase events (Ch 7)."""
    click.echo(f"🛒 Streaming {count} purchase events for campaign '{campaign}'")
    run_cdc_purchases(count, interval, campaign)
    click.echo("✅ Flash sale purchases complete.")


@cli.command()
@click.option("--count", default=1000, help="Number of login events")
@click.option("--interval", default=0.3, help="Seconds between events")
def logins(count, interval):
    """Stream login events to Kafka for Flink processing (Ch 8)."""
    click.echo(f"🔐 Streaming {count} login events")
    run_login_events(count, interval)
    click.echo("✅ Login events complete.")


@cli.command()
@click.option("--count", default=200, help="Number of customer reviews")
def reviews(count):
    """Seed customer reviews into Postgres for pgvector search (Ch 10)."""
    click.echo(f"📝 Generating {count} customer reviews")
    run_reviews_seed(count)
    click.echo("✅ Reviews seeded.")


if __name__ == "__main__":
    cli()
