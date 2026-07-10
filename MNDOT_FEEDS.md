# MnDOT Feed Access

Verified on July 6-7, 2026.

## Live FEU-g feed

- Hub index: <https://mn.carsprogram.org/hub/index.jsf>
- FEU-g XML feed: <https://mn.carsprogram.org/hub/data/feu-g.xml>
- FEU-g event index: <https://mn.carsprogram.org/hub/data/feu-g/index>
- FEU-g schema: <https://mn.carsprogram.org/hub/schemas/FEU-g.xsd>

The FEU-g XML endpoint is protected with HTTP Basic auth. A `HEAD` request against the feed returned `200 OK` on July 6, 2026 when using the provided MnDOT credentials.

## 11Writer Forte workflow

Set the MnDOT password in an environment variable instead of hardcoding it into source metadata:

```powershell
$env:ELEVENWRITER_MNDOT_PASSWORD = "<agency password>"
```

Create the managed source:

```powershell
elevenwriter add-source-http-xml `
  mndot-feu-g `
  https://mn.carsprogram.org/hub/data/feu-g.xml `
  mndot-feu-g `
  --basic-auth-username bconners `
  --basic-auth-password-env ELEVENWRITER_MNDOT_PASSWORD `
  --retry-attempts 3 `
  --skip-unchanged true
```

Then run it:

```powershell
elevenwriter run-source <source_id>
```

`http_xml` sources are materialized as JSON observations. For FEU-g records, the importer currently lifts out:

- `event_id`
- `status`
- `route_designator`
- `observed_at`
- first latitude/longitude pair
- normalized text/title fields for search and review

## Archived loop-detector data

MnDOT's public IRIS archive is Mayfly:

- Docs: <https://data.dot.state.mn.us/mayfly/>
- District list: <https://data.dot.state.mn.us/mayfly/districts>

Example endpoints:

- `https://data.dot.state.mn.us/mayfly/detectors?date=20260706`
- `https://data.dot.state.mn.us/mayfly/counts?date=20260706&detector=363`
- `https://data.dot.state.mn.us/mayfly/speed?date=20260706&detector=363`
- `https://data.dot.state.mn.us/mayfly/occupancy?date=20260706&detector=363`

Mayfly is historical/archive data, not the authenticated live FEU-g feed.

## Cameras

Public camera browsing is available at:

- <https://511mn.org/list/cameras>

The current 511 frontend bundle also references MnDOT camera/location APIs under:

- `https://public.carsprogram.org/mn/prod/cameras_v1/api`
- `https://public.carsprogram.org/mn/prod/locations_v1/api`

Those API bases were inferred from the public 511 frontend bundle on July 6, 2026. Exact camera endpoint paths were not fully derived here, so treat them as leads rather than finished integration points.
