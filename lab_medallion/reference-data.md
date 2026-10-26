# Reference data

`states.csv` resolves the subject's missing `ref('states')` resource. It contains 62 USPS state, territory, freely associated state and military abbreviations. Source: https://pe.usps.com/text/pub28/28apb.htm.

`zip_states.csv` contains 40,979 distinct ZIP/state pairs from https://download.geonames.org/export/zip/US.zip, retrieved on 2026-10-14 UTC. Archive SHA-256: `4880354626f45bb8028c09c4a126e3ae06158ff5f52232deac13e707dc3a6858`. Source description and field layout: https://download.geonames.org/export/zip/.

GeoNames data is licensed CC BY 4.0: https://creativecommons.org/licenses/by/4.0/. Credit: GeoNames, https://www.geonames.org/. I used this geographical reference for the exercise, although it does not establish whether USPS can deliver to an address. Unknown ZIPs are reported separately from known ZIP/state mismatches, since a missing entry does not prove that an address is invalid. AA/AE/AP military ZIPs are outside this US geographical mapping and are excluded from the check.

I versioned the snapshot as a seed so repeated runs use the same reference. For a reference update, I would compare the new snapshot with the stored one before loading it with `dbt seed --full-refresh` and running the tests again. The mapping can change a validation result even when the bronze data has not changed, which is why historical validation also needs the reference date or a mapping with effective dates.
