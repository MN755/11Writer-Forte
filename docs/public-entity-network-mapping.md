# Public entity and network mapping

## Purpose and operating boundary

Public entity and network mapping turns attributed public records into an inspectable
map of **candidate identities, assertions, and evidence**. It is designed to answer
bounded questions such as, “what publicly documented entities have a cited connection
to this jurisdiction during this time period?” It must not produce an accusation graph
from name similarity, an LLM hunch, or a pile of copied headlines. That would be a
confidence UI wrapped around vibes, which is not evidence.

This roadmap owns canonicalization, identity and relationship assertions, public-record
adapters, graph query results, provenance, confidence, redaction, and coverage
reporting. It publishes the stable evidence interface below for investigation, watch,
event, and report workflows. It does not own visual processing, narrative generation,
source delivery, or a map frontend.

The existing `entities` and `entity_observation_links` records remain the compatibility
baseline for rule-based entity resolution. The versioned interface in this document is
the forward contract for public entity and relationship assertions; consumers must not
infer a relationship from the existing observation link alone.

## Public-source policy

Only lawful, no-login public material may be adapted. Eligible categories include:

| Source category | Typical public material | Required handling |
| --- | --- | --- |
| Government and public institution | agencies, registries, notices, sanctions/designations, regulatory or procurement records | Record the publishing institution and jurisdiction; distinguish a designation from a conviction. |
| Court and legal record | published dockets, judgments, orders, filings, and case metadata | Keep case number, court, publication date, procedural posture, and any later disposition. |
| Financial or corporate record | public company/charity registers and official filings | Preserve registry identifier and the exact relationship/role period. |
| Public dataset/API/feed | documented JSON, CSV, XML, GeoJSON, ArcGIS, CKAN, RSS/Atom, or other open endpoint | Pin adapter and schema versions; retain the source record and field mapping. |
| Publisher or news organization | attributed reporting, corrections, and public archives | Treat reporting as reporting, not an independently established fact; do not count syndication/reprints as distinct sources. |
| Public website, social, or video page | a platform page reachable without authentication and allowed by its terms | Capture only the accessible public claim and citation; do not evade restrictions or treat a profile as identity proof by itself. |

No adapter may bypass login, payment, rate limits, robots policy, access controls, or
platform restrictions. It must not collect credentials, private messages, breach data,
data broker profiles, doxxing material, scraped home addresses, routine locations, or
operationally actionable personal data. A source that becomes gated, changes terms, or
returns a schema outside its adapter contract is disabled/quarantined rather than
“handled creatively.”

### Source formats and jurisdiction support

Forte’s managed-source layer currently supports local files and public `http_json`,
`http_jsonl`, `http_text`, `http_xml`, `http_csv`, `rss`,
`arcgis_feature_json`, `ckan_package_search`, `web_search`, `web_crawl`, and
`web_discovery` sources. A format being ingestible does **not** make every endpoint a
permitted entity-mapping source: it still needs a public-access decision, a source
policy, and a versioned adapter.

The mapping model is jurisdiction-neutral, not jurisdiction-complete. It accepts
public records from any jurisdiction where the operator has confirmed lawful access,
but Forte makes no claim of exhaustive coverage, legal equivalence, translation
quality, or availability for any country, state/province, municipality, tribunal, or
language. Jurisdiction must be source-supplied or explicitly labeled as analyst
context; it must never be guessed from a name.

Use this normalized shape when an adapter can support it:

```json
{
  "country_code": "US",
  "subdivision_code": "US-MN",
  "name": "Minnesota, United States",
  "authority": "Hennepin County District Court",
  "authority_kind": "court",
  "source_value": "Hennepin County, Minnesota"
}
```

`country_code` and `subdivision_code` use ISO-style codes when known; all original
labels, scripts, and transliterations remain in the cited record. A missing code is
`null`, not an invitation to fabricate precision. Geography may be a jurisdiction,
source-supplied feature, event location, or `unknown`; it is never a home-location
inference.

## Source policy and schema adapters

Every adapter is tied to a source policy before it can produce a visible assertion.
The policy is reviewable configuration, not an implicit trust score:

```json
{
  "policy_version": "public-record-policy@v1",
  "source_id": "minn-court-opinions",
  "allowed_domains": ["www.mncourts.gov"],
  "access": "public_no_login",
  "jurisdictions": ["US", "US-MN"],
  "permitted_claim_types": ["legal_case", "judgment", "official_designation"],
  "adapter": "court-opinion@v1",
  "schema_fingerprint": "sha256:...",
  "retention_class": "public_record",
  "enabled": true
}
```

An adapter declares its source family, supported jurisdictions/languages, expected
schema fingerprint(s), canonical field mappings, identifier rules, temporal parsing,
and failure policy. It emits a normalized record only when required fields pass
validation. A changed or incomplete schema must produce a visible `quarantined` or
`adapter_failed` result with its raw artifact/custody reference, rather than silently
dropping fields or publishing an unsupported claim.

Minimum adapter output fields are:

| Field | Requirement |
| --- | --- |
| `adapter_version`, `source_policy_version`, `schema_fingerprint` | Make transformations reproducible and schema changes detectable. |
| `source_artifact_id`, `normalized_record_id`, `content_hash` | Bind facts to a retained/retrievable evidence chain. |
| `source_uri`, `publisher`, `retrieved_at`, `published_at` | Separate publisher, retrieval, and publication time. |
| `jurisdiction`, `language`, `record_identifiers` | Preserve legal/locale context and strong identifiers. |
| `field_provenance` | Map every normalized identity or claim field to a source path/span. |
| `warnings` and `review_state` | Surface missing context, parsing uncertainty, and manual review needs. |

Canonicalization is deterministic and loss-aware: it normalizes whitespace, Unicode,
case, script/transliteration variants, identifier formatting, dates, and known aliases
while retaining the original value. Automatic identity merges require a strong public
identifier (for example, a registry ID, case-linked official ID, IMO/MMSI, tail number,
or stable account identifier) or independently corroborated contextual evidence. A
same-name match alone remains separate candidates. Existing resolution signals such as
MMSI, IMO, registration, callsign, account handle, email, and name are useful inputs;
they are not a license to merge identities without the policy above.

## Stable entity and relationship evidence interface (`v1`)

Roadmaps 01 and 02, investigations, watches, exports, and future map clients consume
this interface. It is deliberately record-oriented: relationships are **assertions**
with supporting evidence, status, and time bounds—not bare graph edges.

All references are stable strings in the form `entity:<id>`, `assertion:<id>`,
`artifact:<id>`, `record:<id>`, or `observation:<id>`. Numeric database keys may be
included for local joins, but consumers must not use them as cross-export identifiers.
`schema_version` is mandatory. Additive fields are allowed within a major version;
renames or semantic changes require a new major version.

### Entity evidence record

```json
{
  "schema_version": "entity-evidence@v1",
  "entity_ref": "entity:person:7b0d5d",
  "entity_type": "person",
  "canonical_name": "Amina Rahman",
  "aliases": [
    {
      "value": "A. Rahman",
      "kind": "reported_alias",
      "status": "asserted",
      "evidence_refs": ["record:court-opinion:2026-113"],
      "confidence": {"score": 0.72, "band": "moderate"}
    }
  ],
  "identity_assertions": [
    {
      "assertion_ref": "assertion:identity:11",
      "identifier_type": "organization_registration",
      "identifier_value": "REDACTED-IN-LOWER-CLEARANCE-EXPORTS",
      "status": "asserted",
      "evidence_refs": ["artifact:sha256:4c..."],
      "confidence": {"score": 0.94, "band": "high"}
    }
  ],
  "redaction_level": "restricted",
  "review_state": "reviewed",
  "evidence_count": 2,
  "provenance_refs": ["artifact:sha256:4c...", "observation:93"]
}
```

Permitted node types are `person`, `organization`, `vessel`, `aircraft`, `vehicle`,
`location`, `account`, `legal_case`, `document`, `event`, and `public_institution`.
New types require a versioned vocabulary entry. `canonical_name` is an indexing label,
not a declaration that similarly named records identify the same real-world person.

### Relationship/claim assertion

```json
{
  "schema_version": "relationship-evidence@v1",
  "assertion_ref": "assertion:relationship:204",
  "subject_ref": "entity:person:7b0d5d",
  "predicate": "role.officer_of@v1",
  "object_ref": "entity:organization:3a44ee",
  "claim_type": "official_record",
  "status": "asserted",
  "review_state": "reviewed",
  "jurisdiction": {"country_code": "US", "subdivision_code": "US-MN"},
  "valid_time": {"start": "2024-01-01", "end": null, "precision": "day"},
  "observed_time": {"start": "2026-07-01T00:00:00Z", "end": null},
  "confidence": {
    "score": 0.91,
    "band": "high",
    "basis": ["official_registry_identifier", "direct_source_record"],
    "independent_source_count": 1,
    "contradiction_count": 0
  },
  "evidence": [
    {
      "artifact_ref": "artifact:sha256:4c...",
      "record_ref": "record:registry:8841",
      "source_uri": "https://public.example/record/8841",
      "publisher": "Example Public Registry",
      "source_policy_version": "public-record-policy@v1",
      "adapter_version": "registry-record@v1",
      "retrieved_at": "2026-07-01T14:20:00Z",
      "published_at": "2026-06-29T00:00:00Z",
      "locator": {"kind": "json_pointer", "value": "/officers/0"},
      "content_hash": "sha256:4c..."
    }
  ],
  "contradicts": [],
  "redaction_level": "restricted"
}
```

`predicate` is a constrained, versioned vocabulary such as `role.officer_of@v1`,
`legal.named_in_case@v1`, `association.reported@v1`, or `ownership.registered_to@v1`.
The exact predicate states the kind of connection; `association.reported@v1` must not
be rendered as ownership, co-conspiracy, affiliation, or criminal responsibility.
Every visible relationship has at least one `evidence` item resolving to an artifact
and normalized record. If it does not, it is not a visible relationship. Period.

Claim type keeps legal and factual distinctions explicit. At minimum, producers use
one of `allegation`, `charge_or_indictment`, `official_designation`, `conviction`,
`judgment`, `official_record`, `reported_association`, `independently_corroborated`,
`denial_or_dispute`, or `unknown`. The claim type is not an automatic credibility
ranking and never establishes guilt by itself.

`status` is one of `candidate`, `asserted`, `disputed`, `retracted`, `superseded`, or
`insufficient_evidence`. Contradictions are separate assertion records linked through
`contradicts`; they are not overwritten or averaged away.

## Confidence and disagreement semantics

Confidence represents support for this exact identity or relationship assertion under
the recorded evidence. It is **not** a probability of guilt, a truth guarantee, a
prominence score, or a signal that a person should be investigated.

Scores are in `[0, 1]` and must include their basis. Default display bands are:

| Score | Band | Meaning |
| --- | --- | --- |
| `0.00–0.24` | very low | Candidate only; do not use for graph conclusions. |
| `0.25–0.49` | low | Some attribution exists, but material ambiguity or limited support remains. |
| `0.50–0.74` | moderate | Attributed support with disclosed limitations; never erase disagreement. |
| `0.75–0.89` | high | Strong direct/independent support for the stated claim, not a broader inference. |
| `0.90–1.00` | very high | Strong identifiers or direct official evidence with clear scope; still subject to correction. |

Inputs may include source policy eligibility, directness, identifier strength, temporal
fit, jurisdiction fit, independent-source diversity, corroboration, adapter quality,
and review state. Deductions include contradictory evidence, stale records, unresolved
same-name collisions, source corrections, missing dates, and source dependence.
Reprints, syndicated articles, copied databases, or several pages derived from the
same artifact count as one evidence lineage, not multiple independent sources.

Graph paths, clusters, and rankings must surface confidence bands and disputed edges.
A path that relies on a `disputed` relationship is visibly marked disputed; a result
with weak coverage returns `insufficient_evidence`, not an empty-looking answer that
pretends the subject has no connections.

## Privacy, redaction, and lifecycle

Public availability does not make every datum appropriate to expose. Default outputs
minimize sensitive personal information and obey the record’s `redaction_level`:

| Level | Export/query behavior |
| --- | --- |
| `public` | May be included in public-clearance output if it also satisfies the source policy and privacy rules. |
| `restricted` | Include only when the caller explicitly permits restricted output; remove from public exports. |
| `confidential` | Exclude from public/restricted outputs; requires an authorized confidential workflow. |
| `secret` | Exclude except from a specifically authorized secret workflow. |

Every entity, assertion, evidence item, and derived result inherits the strictest
applicable redaction level. A lower-clearance request filters linked records rather
than leaking them through counts, labels, aliases, relationship labels, source URLs, or
map coordinates. If filtering makes a result misleading, return a redacted/coverage
notice rather than filling the gap with an inference.

Never expose or derive home addresses, precise residence/work patterns, contact
credentials, private account identifiers, protected characteristics, or real-time
location. Coordinates should be omitted, generalized to the lawful source’s published
jurisdiction, or represented as a coarse event area as policy requires. An analyst may
not “solve” a missing identifier by joining to private or breach-derived data.

Raw artifacts follow the universal artifact policy: source bytes are local first,
content-hashed, custody-logged, retention-classed, and eligible for legal hold.
Normalized facts, assertion indexes, and evidence references may outlive raw bytes only
when their artifact hash, transform chain, expiry/hold state, and source policy remain
inspectable. When a backing artifact expires or becomes unavailable, the assertion
remains labeled with that provenance gap and cannot be promoted as fresh evidence.

## Coverage and limitation reporting

Every graph/profile/slice response includes a `coverage` object. It names what was
searched and, just as important, what was not. Minimum fields are:

```json
{
  "coverage": {
    "requested_jurisdictions": ["US-MN"],
    "included_source_policies": ["public-record-policy@v1"],
    "excluded_source_reasons": {"gated": 2, "schema_quarantined": 1, "redaction": 3},
    "time_window": {"start": "2024-01-01", "end": "2026-07-01"},
    "languages": ["en"],
    "artifact_freshness_cutoff": "2026-06-01T00:00:00Z",
    "independent_evidence_lineages": 4,
    "known_gaps": ["No supported county-court adapter for this jurisdiction."],
    "result_status": "partial"
  }
}
```

Allowed `result_status` values are `complete_for_declared_coverage`, `partial`,
`insufficient_evidence`, and `redacted`. “Complete” means complete only for the stated
sources, dates, policies, and query limits—not complete knowledge of a person, network,
or jurisdiction. Known gaps include unsupported languages, unavailable archives,
uncovered courts/registries, expired artifacts, rejected adapters, no-login policy
exclusions, and evidence filtered by redaction.

## Examples

### Same-name collision: retain ambiguity

Two news records name “Jordan Lee,” but have no shared official identifier, date of
birth, organization identifier, case ID, or independently corroborated context. The
adapter emits two `candidate` identity assertions, both with `low` confidence. The
network response returns `insufficient_evidence` for a requested merge and cites the
ambiguity. It does not produce one helpful-looking supernode because the UI wants fewer
circles.

### Corroborated public role with a credible dispute

An official registry states that a person was an officer of an organization from January
through December 2024. A later court filing disputes the appointment date. Forte keeps
both cited assertions: the registry assertion is `asserted`; the filing is
`disputed`/`denial_or_dispute` as appropriate; each includes its dates, jurisdiction,
and source span. A 2024 relationship path can show the asserted role while visibly
annotating the date dispute. A query outside the cited time range does not imply that
the role continued.

### Minimal network-slice response

```json
{
  "schema_version": "network-slice@v1",
  "query": {
    "seed_entity_refs": ["entity:organization:3a44ee"],
    "jurisdictions": ["US-MN"],
    "valid_time": {"start": "2024-01-01", "end": "2024-12-31"},
    "max_redaction_level": "public"
  },
  "entities": [{"entity_ref": "entity:organization:3a44ee", "evidence_count": 4}],
  "relationship_assertions": [],
  "evidence_count": 4,
  "coverage": {
    "result_status": "insufficient_evidence",
    "known_gaps": ["All candidate relationships were restricted or unsupported by a cited artifact."]
  }
}
```

An empty `relationship_assertions` list is therefore not a denial of relationships; the
coverage status explains why no relationship was safely returned.

## Consumer requirements

Consumers in investigation, watch, event, report, and export workflows must preserve
`schema_version`, assertion/entity references, evidence references, claim type,
status, confidence basis, time/jurisdiction, redaction level, and coverage object.
They must not convert an assertion into a bare “fact,” downgrade its redaction, hide a
contradiction, or call a result comprehensive without its declared coverage. When a
consumer creates a derived event/product, it records the source assertion references
and source artifact hashes so snapshot/restore and event exports retain provenance.
