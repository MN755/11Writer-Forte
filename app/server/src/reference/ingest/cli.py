from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from src.config.settings import get_settings
from src.reference.db import session_scope
from src.reference.ingest.dataset_manifest import DatasetManifest
from src.reference.ingest.parsers import PARSERS
from src.reference.ingest.staging import prepare_source
from src.reference.repository import ReferenceRepository
from src.services.ops_audit_service import init_ops_audit_db, record_provenance_event


def run_cli(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest canonical geospatial reference datasets.")
    parser.add_argument("dataset", choices=sorted(PARSERS.keys()))
    parser.add_argument("source_path", help="Directory containing dataset files.")
    parser.add_argument("--database-url", default="sqlite:///./data/reference.db")
    parser.add_argument("--version", default="local")
    parser.add_argument("--coverage", default="global-core")
    parser.add_argument("--checksum", default=None)
    parser.add_argument("--source-mode", choices=["local", "remote"], default="local")
    parser.add_argument("--remote-url", default=None)
    parser.add_argument("--staging-root", default="./data/reference_staging")
    args = parser.parse_args(list(argv) if argv is not None else None)
    runtime_settings = get_settings()

    manifest = DatasetManifest(
        name=args.dataset,
        version=args.version,
        coverage=args.coverage,
        checksum=args.checksum,
        source_mode=args.source_mode,
        precedence=10,
        parser_name=args.dataset,
        source_path=Path(args.source_path) if args.source_mode == "local" else None,
        expected_files=_expected_files(args.dataset),
        remote_url=args.remote_url,
    )
    source_path = prepare_source(manifest, Path(args.staging_root))
    records = PARSERS[args.dataset](source_path, args.version)
    provenance_kwargs = {
        "subsystem": "reference",
        "event_kind": "reference_dataset_ingest",
        "operation": "upsert_records",
        "status": "completed",
        "actor": "11writer-cli",
        "subject_type": "reference_dataset",
        "subject_id": f"{args.dataset}:{args.version}",
        "source_uri": args.remote_url if args.source_mode == "remote" else str(source_path),
        "input_refs": [str(source_path)],
        "output_refs": [f"reference_dataset_load:{args.dataset}:{args.version}"],
        "chain_of_custody": [
            f"source_mode={args.source_mode}",
            f"staging_root={args.staging_root}",
            f"reference_database_url={args.database_url}",
        ],
    }
    if args.database_url == runtime_settings.source_discovery_database_url:
        init_ops_audit_db(args.database_url)
    with session_scope(args.database_url) as session:
        repository = ReferenceRepository(session)
        count = repository.upsert_records(
            records=records,
            dataset_name="faa-fixes" if args.dataset == "fixes" else args.dataset,
            dataset_version=args.version,
            coverage=args.coverage,
            checksum=manifest.checksum,
            source_path=str(source_path),
            notes=f"Loaded via {args.dataset} CLI importer.",
        )
        if args.database_url == runtime_settings.source_discovery_database_url:
            record_provenance_event(
                runtime_settings,
                summary=f"Ingested {count} records from {args.dataset}.",
                metadata={
                    "dataset": args.dataset,
                    "version": args.version,
                    "coverage": args.coverage,
                    "checksum": args.checksum,
                    "record_count": count,
                },
                session=session,
                **provenance_kwargs,
            )
    if args.database_url != runtime_settings.source_discovery_database_url:
        record_provenance_event(
            runtime_settings,
            summary=f"Ingested {count} records from {args.dataset}.",
            metadata={
                "dataset": args.dataset,
                "version": args.version,
                "coverage": args.coverage,
                "checksum": args.checksum,
                "record_count": count,
            },
            **provenance_kwargs,
        )
    print(f"Ingested {count} records from {args.dataset}.")


def main() -> None:
    run_cli()


if __name__ == "__main__":
    main()


def _expected_files(dataset_name: str) -> tuple[str, ...]:
    if dataset_name == "ourairports":
        return ("airports.csv", "runways.csv", "navaids.csv")
    if dataset_name == "places":
        return ("regions.geojson", "places.json")
    if dataset_name == "fixes":
        return ("fixes.csv",)
    if dataset_name == "airport-codes":
        return ("airport-codes.json",)
    return ()
