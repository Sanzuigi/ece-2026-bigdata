# Reference data

`states.csv` resolves the subject's missing `ref('states')` resource. It contains 62 USPS state, territory, freely associated state and military abbreviations. Source: https://pe.usps.com/text/pub28/28apb.htm.

`zip_states.csv` contains 40,979 distinct ZIP/state pairs from https://download.geonames.org/export/zip/US.zip, retrieved on 2026-10-14 UTC. Archive SHA-256: `4880354626f45bb8028c09c4a126e3ae06158ff5f52232deac13e707dc3a6858`. Source description and field layout: https://download.geonames.org/export/zip/.

GeoNames data is licensed CC BY 4.0: https://creativecommons.org/licenses/by/4.0/. Credit: GeoNames, https://www.geonames.org/. This public geographical reference is useful for this exercise but is not an authoritative USPS deliverability service. Unknown ZIPs are reported separately from known ZIP/state mismatches; missing coverage must not be called proof of an invalid address. AA/AE/AP military ZIPs are outside this US geographic mapping and are explicitly excluded from that check.

The snapshot is versioned as a seed so runs use the same reference. Review a new snapshot and its differences, then reload it with `dbt seed --full-refresh` and rerun tests. A changed mapping can change the quality result without the bronze data changing. Keep snapshot dates or effective-date mappings when validating historical addresses.
