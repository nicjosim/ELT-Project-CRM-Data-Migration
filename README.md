# ELT Project - CRM Data Migration

This project transforms a single Excel input into clean, deduplicated investor records and produces registry-ready data for downstream use. The solution is fully config-driven and structured to reflect practical data engineering workflows, with clear separation of concerns across ingestion, transformation, validation, and entity resolution.

The pipeline begins with ingestion, where the Excel file is read using a configurable header row. The first valid table found is extracted and written directly to a raw CSV without any transformation, preserving the original data for traceability and audit purposes.

In the standardization stage, raw columns are renamed using mappings defined in config.yaml. Investor attributes such as names, emails, phone numbers, dates of birth, addresses, cities, countries, percentages, and tax identifiers are normalized into consistent formats. Where country information is missing, it is inferred from city values when possible, and country codes are derived from the resolved country. Tax rate values are assumed to represent PIR percentages based on external references. The output of this stage is a clean and consistent standardized.csv.

The merge stage focuses on de-duplication and identity resolution. Potential duplicate investors are identified using a 3-of-5 matching strategy across phone number, date of birth, normalized address, tax identification number, and email. Any records sharing a matching triple are grouped into the same cluster, with union–find used to resolve chained duplicates. When merging, the most complete record within a cluster is selected as the base, and remaining fields are resolved using majority voting across the cluster.

Registry construction then derives registry-ready records by linking investment entries to the merged investor dataset using stable investor identifiers. Where transaction data cannot be confidently resolved, notes are populated to flag the record for manual review rather than silently discarding or guessing values.

Schema enforcement is handled using schema.yaml, which defines required and optional fields for both investors and registry datasets. Any missing required values are explicitly filled with a placeholder value (NOT AVAILABLE) to make data quality issues visible and auditable.

The pipeline is executed end-to-end in the following order: ingest.py, standardize.py, merge.py, and build_registry.py. Configuration such as file paths, column mappings, and ingestion rules are controlled via config.yaml, while rules.py centralizes normalization logic for dates, phone numbers, countries, addresses, percentages, and merge behavior to ensure consistent processing across all stages.

The final outputs include raw.csv as the untouched ingestion output, standardized.csv containing cleaned investor data, merged.csv or investors.csv with deduplicated investor records, and raw_registry.csv or registry.csv containing registry-ready investment records.
