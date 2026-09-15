"""Bronze JSON -> typed, validated silver frames plus a quarantine of rejects.

The pipeline is *reject, don't coerce*: a record that fails the structural
contract (pydantic) or the value contract (pandera) is written to the
quarantine table with the reason and the original record, and the rest of
the partition is published. Only a partition that is too thin to be
trustworthy fails the run outright (``PartitionQualityError``).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pydantic import ValidationError

from gridcarbon.config import Settings, get_settings
from gridcarbon.domain import DNO_REGIONS, WEATHER_VARIABLES, Partition
from gridcarbon.logging import get_logger
from gridcarbon.silver import schemas
from gridcarbon.silver.models import (
    DataEnvelope,
    NationalGenerationPeriod,
    NationalIntensityPeriod,
    OpenMeteoLocation,
    RegionalIntensityPeriod,
)

log = get_logger(__name__)


class PartitionQualityError(RuntimeError):
    """The partition is not fit to publish (e.g. most of the day is missing)."""


@dataclass(frozen=True)
class Lineage:
    partition: Partition
    fetched_at: datetime
    run_id: str
    bronze_sha256: str

    def columns(self) -> dict[str, Any]:
        return {
            "partition_date": pd.Timestamp(self.partition.day),
            "fetched_at": pd.Timestamp(self.fetched_at).tz_convert("UTC"),
            "run_id": self.run_id,
            "bronze_sha256": self.bronze_sha256,
        }


@dataclass
class ParsedTable:
    name: str
    frame: pd.DataFrame
    quarantine: pd.DataFrame
    rows_in: int
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def rows_out(self) -> int:
        return len(self.frame)

    @property
    def rows_quarantined(self) -> int:
        return len(self.quarantine)


class _Quarantine:
    def __init__(self, table: str, lineage: Lineage) -> None:
        self.table = table
        self.lineage = lineage
        self.rows: list[dict[str, Any]] = []

    def add(self, reason: str, record: Any) -> None:
        self.rows.append(
            {
                "table": self.table,
                "reason": reason[:500],
                "record": json.dumps(record, default=str)[:4000],
                **self.lineage.columns(),
            }
        )

    def frame(self) -> pd.DataFrame:
        if not self.rows:
            return _empty_quarantine()
        return schemas.QUARANTINE.validate(pd.DataFrame(self.rows))


def _empty_quarantine() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "table": pd.Series(dtype=str),
            "reason": pd.Series(dtype=str),
            "record": pd.Series(dtype=str),
            "partition_date": pd.Series(dtype="datetime64[ns]"),
            "fetched_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "run_id": pd.Series(dtype=str),
            "bronze_sha256": pd.Series(dtype=str),
        }
    )


def _summarise_validation_error(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors()[:3]:
        loc = ".".join(str(p) for p in err["loc"])
        parts.append(f"{loc}: {err['msg']}")
    return "; ".join(parts)


def _apply_schema(
    schema: pa.DataFrameSchema, frame: pd.DataFrame, quarantine: _Quarantine
) -> pd.DataFrame:
    """Validate lazily; move every failing row to quarantine; return the clean rest."""
    if frame.empty:
        return schema.validate(frame.reindex(columns=list(schema.columns)), lazy=False)
    try:
        return schema.validate(frame, lazy=True)
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases
        bad_index = failures["index"].dropna().astype(int).unique()
        reasons = (
            failures.dropna(subset=["index"])
            .assign(index=lambda f: f["index"].astype(int))
            .groupby("index")
            .apply(
                lambda g: "; ".join(
                    f"{c}: {chk}" for c, chk in zip(g["column"], g["check"], strict=True)
                )[:500],
                include_groups=False,
            )
        )
        for idx in bad_index:
            quarantine.add(
                f"schema {schema.name}: {reasons.loc[idx]}",
                frame.loc[idx].to_dict(),
            )
        # Dataframe-level checks (no index) mean the partition itself is malformed.
        df_level = failures[failures["index"].isna()]
        if not df_level.empty and len(bad_index) == 0:
            raise PartitionQualityError(
                f"{schema.name}: dataframe-level check failed: "
                + "; ".join(df_level["check"].astype(str).unique()[:3])
            ) from exc
        remaining = frame.drop(index=bad_index)
        return schema.validate(remaining, lazy=False)


def _in_window(frame: pd.DataFrame, column: str, partition: Partition) -> pd.Series:
    start = pd.Timestamp(partition.start)
    end = pd.Timestamp(partition.end)
    return (frame[column] >= start) & (frame[column] < end)


# --------------------------------------------------------------------------
# Carbon Intensity datasets
# --------------------------------------------------------------------------


def _envelope(payload: Any) -> list[dict[str, Any]]:
    envelope = DataEnvelope.model_validate(payload)
    return [dict(item) for item in envelope.data]


def parse_national_intensity(
    payload: Any, lineage: Lineage, settings: Settings
) -> list[ParsedTable]:
    quarantine = _Quarantine("intensity_national", lineage)
    raw = _envelope(payload)
    rows: list[dict[str, Any]] = []
    for item in raw:
        try:
            period = NationalIntensityPeriod.model_validate(item)
        except ValidationError as exc:
            quarantine.add(_summarise_validation_error(exc), item)
            continue
        rows.append(
            {
                "period_start": period.from_,
                "period_end": period.to,
                "forecast": period.intensity.forecast,
                "actual": period.intensity.actual,
                "index": period.intensity.index,
                **lineage.columns(),
            }
        )
    frame = pd.DataFrame(rows)
    trimmed = 0
    if not frame.empty:
        frame["period_start"] = pd.to_datetime(frame["period_start"], utc=True)
        frame["period_end"] = pd.to_datetime(frame["period_end"], utc=True)
        mask = _in_window(frame, "period_start", lineage.partition)
        trimmed = int((~mask).sum())
        frame = (
            frame[mask].drop_duplicates(subset=["period_start"], keep="last").reset_index(drop=True)
        )
    clean = _apply_schema(schemas.INTENSITY_NATIONAL, frame, quarantine)
    _require_periods(clean, "period_start", settings.min_periods_per_day, "intensity_national")
    return [
        ParsedTable(
            "intensity_national",
            clean,
            quarantine.frame(),
            rows_in=len(raw),
            details={"trimmed_outside_window": trimmed},
        )
    ]


def parse_national_generation(
    payload: Any, lineage: Lineage, settings: Settings
) -> list[ParsedTable]:
    quarantine = _Quarantine("generation_national", lineage)
    raw = _envelope(payload)
    rows: list[dict[str, Any]] = []
    for item in raw:
        try:
            period = NationalGenerationPeriod.model_validate(item)
        except ValidationError as exc:
            quarantine.add(_summarise_validation_error(exc), item)
            continue
        if not _mix_sums_to_100(period.generationmix, settings):
            quarantine.add("generation mix does not sum to 100", item)
            continue
        for share in period.generationmix:
            rows.append(
                {
                    "period_start": period.from_,
                    "period_end": period.to,
                    "fuel": share.fuel,
                    "perc": share.perc,
                    **lineage.columns(),
                }
            )
    frame = pd.DataFrame(rows)
    trimmed = 0
    if not frame.empty:
        frame["period_start"] = pd.to_datetime(frame["period_start"], utc=True)
        frame["period_end"] = pd.to_datetime(frame["period_end"], utc=True)
        mask = _in_window(frame, "period_start", lineage.partition)
        trimmed = int((~mask).sum())
        frame = (
            frame[mask]
            .drop_duplicates(subset=["period_start", "fuel"], keep="last")
            .reset_index(drop=True)
        )
    clean = _apply_schema(schemas.GENERATION_NATIONAL, frame, quarantine)
    _require_periods(clean, "period_start", settings.min_periods_per_day, "generation_national")
    return [
        ParsedTable(
            "generation_national",
            clean,
            quarantine.frame(),
            rows_in=len(raw),
            details={"trimmed_outside_window": trimmed},
        )
    ]


def parse_regional_intensity(
    payload: Any, lineage: Lineage, settings: Settings
) -> list[ParsedTable]:
    q_int = _Quarantine("intensity_regional", lineage)
    q_gen = _Quarantine("generation_regional", lineage)
    raw = _envelope(payload)
    int_rows: list[dict[str, Any]] = []
    gen_rows: list[dict[str, Any]] = []
    for item in raw:
        try:
            period = RegionalIntensityPeriod.model_validate(item)
        except ValidationError as exc:
            q_int.add(_summarise_validation_error(exc), item)
            continue
        for region in period.regions:
            int_rows.append(
                {
                    "period_start": period.from_,
                    "period_end": period.to,
                    "region_id": region.regionid,
                    "forecast": region.intensity.forecast,
                    "index": region.intensity.index,
                    **lineage.columns(),
                }
            )
            if not _mix_sums_to_100(region.generationmix, settings):
                q_gen.add(
                    "generation mix does not sum to 100",
                    {
                        "from": item.get("from"),
                        "regionid": region.regionid,
                        "mix": [s.model_dump() for s in region.generationmix],
                    },
                )
                continue
            for share in region.generationmix:
                gen_rows.append(
                    {
                        "period_start": period.from_,
                        "period_end": period.to,
                        "region_id": region.regionid,
                        "fuel": share.fuel,
                        "perc": share.perc,
                        **lineage.columns(),
                    }
                )

    int_frame = pd.DataFrame(int_rows)
    gen_frame = pd.DataFrame(gen_rows)
    trimmed = 0
    for frame_name, frame, keys in (
        ("int", int_frame, ["period_start", "region_id"]),
        ("gen", gen_frame, ["period_start", "region_id", "fuel"]),
    ):
        if frame.empty:
            continue
        frame["period_start"] = pd.to_datetime(frame["period_start"], utc=True)
        frame["period_end"] = pd.to_datetime(frame["period_end"], utc=True)
        mask = _in_window(frame, "period_start", lineage.partition)
        if frame_name == "int":
            trimmed = int((~mask).sum())
        kept = frame[mask].drop_duplicates(subset=keys, keep="last").reset_index(drop=True)
        if frame_name == "int":
            int_frame = kept
        else:
            gen_frame = kept

    int_clean = _apply_schema(schemas.INTENSITY_REGIONAL, int_frame, q_int)
    gen_clean = _apply_schema(schemas.GENERATION_REGIONAL, gen_frame, q_gen)
    _require_periods(int_clean, "period_start", settings.min_periods_per_day, "intensity_regional")
    return [
        ParsedTable(
            "intensity_regional",
            int_clean,
            q_int.frame(),
            rows_in=len(raw),
            details={"trimmed_outside_window": trimmed},
        ),
        ParsedTable("generation_regional", gen_clean, q_gen.frame(), rows_in=len(raw)),
    ]


def parse_carbon_factors(payload: Any, lineage: Lineage, settings: Settings) -> list[ParsedTable]:
    quarantine = _Quarantine("carbon_factors", lineage)
    raw = _envelope(payload)
    rows: list[dict[str, Any]] = []
    for item in raw:
        for label, value in item.items():
            if not isinstance(value, int | float) or isinstance(value, bool):
                quarantine.add(f"{label}: factor is not numeric", {label: value})
                continue
            rows.append({"fuel_label": str(label), "gco2_per_kwh": int(value), **lineage.columns()})
    frame = pd.DataFrame(rows)
    clean = _apply_schema(schemas.CARBON_FACTORS, frame, quarantine)
    if clean.empty:
        raise PartitionQualityError("carbon_factors: no factors parsed")
    return [ParsedTable("carbon_factors", clean, quarantine.frame(), rows_in=len(rows))]


def _mix_sums_to_100(mix: Iterable[Any], settings: Settings) -> bool:
    total = float(sum(share.perc for share in mix))
    return bool(abs(total - 100.0) <= settings.generation_mix_tolerance_pct)


def _require_periods(frame: pd.DataFrame, column: str, minimum: int, table: str) -> None:
    distinct = int(frame[column].nunique()) if not frame.empty else 0
    if distinct < minimum:
        raise PartitionQualityError(
            f"{table}: only {distinct} distinct periods after validation (minimum {minimum})"
        )


# --------------------------------------------------------------------------
# Open-Meteo
# --------------------------------------------------------------------------


def parse_weather_hourly(
    payload: Any, lineage: Lineage, settings: Settings, region_ids: list[int] | None = None
) -> list[ParsedTable]:
    quarantine = _Quarantine("weather_hourly", lineage)
    locations = payload if isinstance(payload, list) else [payload]
    region_ids = region_ids or [r.region_id for r in DNO_REGIONS]
    rows: list[dict[str, Any]] = []
    all_null = 0
    for position, item in enumerate(locations):
        try:
            loc = OpenMeteoLocation.model_validate(item)
        except ValidationError as exc:
            quarantine.add(_summarise_validation_error(exc), {"location_index": position})
            continue
        index = loc.location_id if len(locations) > 1 else position
        if index >= len(region_ids):
            quarantine.add(f"location_id {index} has no region mapping", {"location_index": index})
            continue
        region_id = region_ids[index]
        hourly = loc.hourly
        for i, stamp in enumerate(hourly.time):
            values = {var: getattr(hourly, var)[i] for var in WEATHER_VARIABLES}
            if all(v is None for v in values.values()):
                all_null += 1
                quarantine.add(
                    "all weather variables null (archive not yet available for this hour)",
                    {"region_id": region_id, "time": stamp},
                )
                continue
            rows.append(
                {
                    "region_id": region_id,
                    "observed_at": pd.Timestamp(stamp, tz="UTC"),
                    **values,
                    **lineage.columns(),
                }
            )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        mask = _in_window(frame, "observed_at", lineage.partition)
        frame = (
            frame[mask]
            .drop_duplicates(subset=["region_id", "observed_at"], keep="last")
            .reset_index(drop=True)
        )
    clean = _apply_schema(schemas.WEATHER_HOURLY, frame, quarantine)
    expected = 24 * len(region_ids)
    if len(clean) < expected // 2:
        raise PartitionQualityError(
            f"weather_hourly: {len(clean)} of {expected} expected hourly rows after validation"
        )
    return [
        ParsedTable(
            "weather_hourly",
            clean,
            quarantine.frame(),
            rows_in=sum(len(loc.get("hourly", {}).get("time", [])) for loc in locations),
            details={"all_null_hours": all_null},
        )
    ]


# --------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------


def parse_dataset(
    dataset: str,
    payload: Any,
    lineage: Lineage,
    settings: Settings | None = None,
    request: dict[str, Any] | None = None,
) -> list[ParsedTable]:
    settings = settings or get_settings()
    match dataset:
        case "national_intensity":
            return parse_national_intensity(payload, lineage, settings)
        case "national_generation":
            return parse_national_generation(payload, lineage, settings)
        case "regional_intensity":
            return parse_regional_intensity(payload, lineage, settings)
        case "carbon_factors":
            return parse_carbon_factors(payload, lineage, settings)
        case "weather_hourly":
            region_ids = (request or {}).get("region_ids")
            return parse_weather_hourly(payload, lineage, settings, region_ids)
    raise ValueError(f"no parser for dataset {dataset!r}")
