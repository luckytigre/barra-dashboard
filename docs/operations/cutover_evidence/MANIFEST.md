# Cutover Evidence Manifest

Generated: 2026-08-23
Baseline commit: `333adbd688ffb6d63ba7b3646578a67b8362351e`
Retention status: retain all evidence in place

This manifest inventories the 85 tracked cutover-evidence artifacts without
moving, deleting, compressing, or rewriting them. Duplicate groups preserve
independent capture-path provenance even when their bytes are identical.

A non-logging high-confidence scan checked the current evidence tree for
private-key blocks, common provider tokens, JWTs, credentialed database URLs,
authorization values, and assigned project secret variables. It found no
credential candidate after excluding placeholders and Markdown punctuation.
This is a hygiene check, not a substitute for GitHub secret scanning or
credential-owner review.

## Summary

- Artifacts: 85
- Bytes: 16880431
- Byte-identical groups: 15
- Duplicate working-tree bytes retained for provenance: 6273030
- Resolved tracked inbound references: 71

## Artifact Inventory

| Path | Bytes | Capture marker | SHA-256 | Git blob | Introduced commit |
| --- | ---: | --- | --- | --- | --- |
| `FRONTEND_AUTH_EXECUTION_20260415T010336Z.md` | 3708 | `20260415T010336Z` | `2e8ba79526115f43cdb818131e08c058d6a5665680d2d6939842f9113bd0a018` | `81f70997ee2c` | `88a53d43614e` |
| `PHASE0_STEP0A_20260412T044539Z.md` | 2375 | `20260412T044539Z` | `0c7a0568aefd88ce02dea24a7a03fa0c9de5ff197825799f30ad6cfac06fff20` | `13d30c9c812b` | `29be4c0e040b` |
| `PHASE0_STEP1_2_20260412T044633Z.md` | 3583 | `20260412T044633Z` | `55ef7f3e0b8e27dbea89c4393b69c881a6f6c3daacce5d676e704918d45dcf99` | `8a13ab05b6a4` | `153dc9139dac` |
| `PHASE0_STEP3_20260412T045000Z.md` | 2934 | `20260412T045000Z` | `8e30c527af291127c2548ffba29e1f29ed0c879a568b109b2a52747fb8351a78` | `eaeecb8345e8` | `899960040f70` |
| `PHASE0_STEP4_5_20260412T045200Z.md` | 1674 | `20260412T045200Z` | `293608fbeb7aa17f15aa6261a9e3b4051dfb3ed996975a673c21eea6ff2d4bd0` | `d971bf27ac43` | `60080e27d44c` |
| `PHASE1_STEP1A_1E_20260412T052000Z.md` | 4286 | `20260412T052000Z` | `a4f4db89627f1c794b02f84852add6bb017722f7d8bc2edad70fac351f771703` | `dd4ed0f57a63` | `76bf2263031d` |
| `PHASE2_STEP2_20260413T074000Z.md` | 3604 | `20260413T074000Z` | `b64587641d967bdfee32826822966529ad17266515bbd3f8c7c91f674b4b3bbb` | `98373aad3f12` | `7fc82b996b73` |
| `PHASE3_STEP1_4_20260413T074500Z.md` | 1619 | `20260413T074500Z` | `85f7653925cfbd4a17b278ad3a2226e162badadd3e0962d8a20cc0d4830400bc` | `1289109d21d0` | `0274193de640` |
| `PHASE3_STEP3_20260414T100000Z.md` | 2989 | `20260414T100000Z` | `e3ca583f0737739686c6bfbbf01123745473be7a10ea7019531ab3e601a8c95a` | `1c5a543ed31e` | `2012c3a08824` |
| `PHASE4_DAY1_20260414T210100Z.md` | 2979 | `20260414T210100Z` | `4b60f8e400d983130b416bf139c6faea9bd776bb040d61a23f55914891399778` | `c4e7689f2ca5` | `84583532d0c7` |
| `PHASE4_ENTRY_20260414T193820Z.md` | 11808 | `20260414T193820Z` | `f3d07d3d6d16d725dc917ab43096bee354cc41acf64300056a6ba2e042e7717d` | `3c9b5d186b2a` | `fe3660ba3d6d` |
| `PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | 5052 | `20260414T215254Z` | `749764bc0ab48954909f21a0141de71af8ec4d389d07a3da06c84ba9ebc62093` | `c142fa0c8f8f` | `05a579c521d0` |
| `PHASE4_STABILITY_AND_ROLLBACK_TEMPLATE.md` | 3307 | `not encoded` | `e1db2bf7fc7dd712822650fb6e5377010a9e2a31e5b3569a8a7371f5b1de8679` | `3130eb8f497e` | `693d6918a6f5` |
| `PHASE4_UPDATE_20260414T201917Z.md` | 2778 | `20260414T201917Z` | `4a3c2a4bf50e96f6c1b4765999d8f7b6f1fa599fe52ed213ead53ee3ff41184e` | `6b58e2179448` | `11eb1e139d82` |
| `phase4_20260414T193820Z/api_cpar_meta.json` | 3553 | `20260414T193820Z` | `2bfef01143529b54b36ea56b1fe45c87f71e6f48e4cb0f8be20e2bd9c225ad33` | `ad23a82b7527` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/api_data_diagnostics.json` | 3184 | `20260414T193820Z` | `97334a5fbd0293b77fb6a9a73cebffe5c17a30fbffa1fefb6d6e6b27813db92d` | `54ca6e9eb462` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/api_health_diagnostics.json` | 1751 | `20260414T193820Z` | `8a192d40a2f6d955f27717101d4ccb4d7052128fe0582e7b0dd1fe12fe0df297` | `6fd35e60a857` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/api_operator_status.json` | 32614 | `20260414T193820Z` | `26075ccd2948cfce8d89d58ac5e7ebb5637ef611eb8600c0db0b58834c1b53d2` | `25794d2889e2` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/api_refresh_status.json` | 11505 | `20260414T193820Z` | `f5262b89e39702e1b336d269e30ce4aeb7025b5853689203f985f094653a69b8` | `8ad756b004c6` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/cloud-topology-check.txt` | 88610 | `20260414T193820Z` | `4fc735bec8f2628b5cc9059fe5b22ff1ba64a9b1fc9067c8379ba6581e388839` | `5a35636d0fdb` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/job_cold_core.json` | 3140 | `20260414T193820Z` | `066ab1b6b5e08a11895f3caf794781540b1464ef21cce7249b182fa7fbae015d` | `e0d3842dfe7f` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/job_core_weekly.json` | 3143 | `20260414T193820Z` | `46f1dfa4c5fb356b75005b530cb7a975d1799c59b7c2171aa6ed38df14e3a395` | `9b961125f0f9` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/job_cpar_build.json` | 3146 | `20260414T193820Z` | `50edfa0d39533223cb84e1870bfd5799d85f29faf4e9ccabd9bee11c580a33f8` | `cd8ec58513cd` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/job_serve_refresh.json` | 3157 | `20260414T193820Z` | `ce5a6988f3c1843ed9108cc10ad1cdf519d1db849727b75594edf80ea607c9d0` | `5c553428403c` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/operator-check.txt` | 88465 | `20260414T193820Z` | `a2eeba1e94a3b8c5494106c95b056e761825f62de3cdc26af95e638e819a99fe` | `5fcfc8845731` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/rollout-bundle-capture.txt` | 1100 | `20260414T193820Z` | `08bd5ebe1fdbd85e65fe53aa1ef4234ffe107a6a0be71af6ddfa098b24989c0d` | `c75b26882cf7` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/service_control.json` | 5511 | `20260414T193820Z` | `a35e3d3a9556f77a9f6fec327355ebcb2e59b5f6d243fb658c4334c0a597cebb` | `41ab2732e1ad` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/service_frontend.json` | 3720 | `20260414T193820Z` | `a6a71138220b7e386ffbf6195ba93724665220dbbc5b986bdd6ded3a18bc1285` | `06cd30e68a7f` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/service_serve.json` | 4834 | `20260414T193820Z` | `963faa6793e50f2ef8d4de4646667b9c9ee50cbd009326a1b892015afbe3f446` | `18c910a20fe7` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/terraform-output.json` | 8092 | `20260414T193820Z` | `7f1d73ce44715b4d562ed3ba4d32aeea8dd910388467aba4ec019e2f1bb4c261` | `c2a4e02d4f50` | `fe3660ba3d6d` |
| `phase4_20260414T193820Z/topology-contract.md` | 818 | `20260414T193820Z` | `1b045a28bf5423a6eb1a40a55ceeb32abc074e8864a1a28ddcd56b468d92af2a` | `88702958fd34` | `fe3660ba3d6d` |
| `phase4_20260414T201840Z/rollout-bundle-capture.txt` | 0 | `20260414T201840Z` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e69de29bb2d1` | `2cccf0f987ae` |
| `phase4_20260414T201840Z/terraform-output.json` | 8092 | `20260414T201840Z` | `7f1d73ce44715b4d562ed3ba4d32aeea8dd910388467aba4ec019e2f1bb4c261` | `c2a4e02d4f50` | `2cccf0f987ae` |
| `phase4_20260414T201840Z/topology-contract.md` | 818 | `20260414T201840Z` | `1b045a28bf5423a6eb1a40a55ceeb32abc074e8864a1a28ddcd56b468d92af2a` | `88702958fd34` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/api_cpar_meta.json` | 3553 | `20260414T201917Z` | `2bfef01143529b54b36ea56b1fe45c87f71e6f48e4cb0f8be20e2bd9c225ad33` | `ad23a82b7527` | `5497eb281473` |
| `phase4_20260414T201917Z/api_data_diagnostics.json` | 3184 | `20260414T201917Z` | `97334a5fbd0293b77fb6a9a73cebffe5c17a30fbffa1fefb6d6e6b27813db92d` | `54ca6e9eb462` | `5497eb281473` |
| `phase4_20260414T201917Z/api_health_diagnostics.json` | 1751 | `20260414T201917Z` | `8a192d40a2f6d955f27717101d4ccb4d7052128fe0582e7b0dd1fe12fe0df297` | `6fd35e60a857` | `d0d21b284b15` |
| `phase4_20260414T201917Z/api_operator_status.json` | 32614 | `20260414T201917Z` | `80ddd966fe45842d0dd839cc54b4683e55a3746a6fb312c53569293ce2a24689` | `88c35e466d52` | `d0d21b284b15` |
| `phase4_20260414T201917Z/api_refresh_status.json` | 11505 | `20260414T201917Z` | `f5262b89e39702e1b336d269e30ce4aeb7025b5853689203f985f094653a69b8` | `8ad756b004c6` | `d0d21b284b15` |
| `phase4_20260414T201917Z/cloud-topology-check.txt` | 88610 | `20260414T201917Z` | `c9046b42a8d31aae27bb3cc2629f88e38e961c6da8dd299127de547cb1ef0604` | `5ace13c1e228` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/day1/api_cpar_meta.json` | 3553 | `20260414T201917Z` | `2bfef01143529b54b36ea56b1fe45c87f71e6f48e4cb0f8be20e2bd9c225ad33` | `ad23a82b7527` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/api_data_diagnostics.json` | 3184 | `20260414T201917Z` | `cd0c525a726b6c2e6ac02f0f012dddc44323b6ab4a90aa92a27edf95d4dc6fa2` | `bab23857fa0c` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/api_health_diagnostics.json` | 6015004 | `20260414T201917Z` | `e3247db2fa39c2c715f759b20ed09154d16b8017a7a0fc14a9ea94214ad35dc2` | `856eef4b94de` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/api_operator_status.json` | 109815 | `20260414T201917Z` | `02dde39d41af5c948747d7983f480489fbd440c7e8303a2f51c3985a8ee0d8c7` | `0e0cf98f3fcc` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/api_operator_status_post_fix.json` | 109910 | `20260414T201917Z` | `fa6912fecff4dc9c9edea7a2db6e74e590ce62f06d91f06a5920fc6a039dcb2a` | `c160ffb6245a` | `30a6acbbaacc` |
| `phase4_20260414T201917Z/day1/api_operator_status_post_source_daily.json` | 109905 | `20260414T201917Z` | `0b79b18e4b6d49476ac853a18f2d2c11a8d978369f578e1001ba378b9590e67a` | `dfc063167a23` | `30a6acbbaacc` |
| `phase4_20260414T201917Z/day1/api_refresh_status.json` | 50107 | `20260414T201917Z` | `e6b699931bf577d9026ea4091b4415e5fbe740adf01b84374fdc41074c32872a` | `ffbf596b71c0` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/api_refresh_status_post_fix.json` | 50107 | `20260414T201917Z` | `e6b699931bf577d9026ea4091b4415e5fbe740adf01b84374fdc41074c32872a` | `ffbf596b71c0` | `30a6acbbaacc` |
| `phase4_20260414T201917Z/day1/api_refresh_status_post_source_daily.json` | 50107 | `20260414T201917Z` | `e6b699931bf577d9026ea4091b4415e5fbe740adf01b84374fdc41074c32872a` | `ffbf596b71c0` | `30a6acbbaacc` |
| `phase4_20260414T201917Z/day1/cloud-topology-check.txt` | 320216 | `20260414T201917Z` | `1b0146ba949730211ab3d0a86f3f6f99dd9b00af3ecd9fa155db8c23437b9488` | `df0408fda937` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/control_api_data_diagnostics.json` | 3184 | `20260414T201917Z` | `cd0c525a726b6c2e6ac02f0f012dddc44323b6ab4a90aa92a27edf95d4dc6fa2` | `bab23857fa0c` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/control_api_health_diagnostics.json` | 6015004 | `20260414T201917Z` | `e3247db2fa39c2c715f759b20ed09154d16b8017a7a0fc14a9ea94214ad35dc2` | `856eef4b94de` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/control_api_operator_status.json` | 109815 | `20260414T201917Z` | `5a7c42a5bc265b2f49428f68d29b05096ad47e5b29b24b257c0822a8e1752427` | `84e79fe418d6` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/control_api_refresh_status.json` | 50107 | `20260414T201917Z` | `e6b699931bf577d9026ea4091b4415e5fbe740adf01b84374fdc41074c32872a` | `ffbf596b71c0` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/job_core_weekly_pv25b.json` | 4240 | `20260414T201917Z` | `6e998112a596d55d3eef4a33b6c25a114b6e25bf4a207c1533dc3e358f71e9b6` | `ac24d79e7335` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/operator-check.txt` | 320071 | `20260414T201917Z` | `55c8224ff28849c6205f16b4bfac8acf1261a9ed0703cd29469fc5c854cf93a6` | `90f892871f20` | `84583532d0c7` |
| `phase4_20260414T201917Z/day1/source-daily-post-fix.txt` | 39130 | `20260414T201917Z` | `34d28bcf5a29c9b5adc26362058df930e7255cd5b577bd0edc9def4e2903308a` | `441151e54123` | `30a6acbbaacc` |
| `phase4_20260414T201917Z/day1/source-daily-rerun.txt` | 61824 | `20260414T201917Z` | `0303cc79a5efd5ea3b6c18b3183fc36358dfe6cdb80d94217a57450f4893c244` | `1bf834d4fbd2` | `30a6acbbaacc` |
| `phase4_20260414T201917Z/day1/source-daily.txt` | 1051628 | `20260414T201917Z` | `1af60f88e784305611c0ef4bf1a881127be7750e5f9f6bf3a4c61c0206b9c465` | `71ff1e16a5b0` | `84583532d0c7` |
| `phase4_20260414T201917Z/job_cold_core.json` | 3140 | `20260414T201917Z` | `066ab1b6b5e08a11895f3caf794781540b1464ef21cce7249b182fa7fbae015d` | `e0d3842dfe7f` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/job_core_weekly.json` | 3143 | `20260414T201917Z` | `46f1dfa4c5fb356b75005b530cb7a975d1799c59b7c2171aa6ed38df14e3a395` | `9b961125f0f9` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/job_cpar_build.json` | 3146 | `20260414T201917Z` | `50edfa0d39533223cb84e1870bfd5799d85f29faf4e9ccabd9bee11c580a33f8` | `cd8ec58513cd` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/job_serve_refresh.json` | 3157 | `20260414T201917Z` | `ce5a6988f3c1843ed9108cc10ad1cdf519d1db849727b75594edf80ea607c9d0` | `5c553428403c` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/operator-check.txt` | 88465 | `20260414T201917Z` | `51fa1b841df4dfeee66a3b8405f00d07db80e356fed1de22e0a110ad06f7e0f5` | `665cfef02fc5` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.forward.retry.txt` | 320406 | `20260414T201917Z` | `69eac02124c49982958785bd155d23b59986c2414174191c661c1ee845cc73ef` | `76ce69fac416` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.forward.txt` | 50525 | `20260414T201917Z` | `bf5c4c3efad8d6c504e84763646af1224f44e799d0a36505aebf055fa313da23` | `1eaa38ab0585` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.rollback.txt` | 320406 | `20260414T201917Z` | `4494367d22686bcf1d16a6a25bb7879db481aa97b3dedff75751dc54df8032b0` | `a3b64aa34089` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/control-revision.forward.yaml` | 931 | `20260414T201917Z` | `6d1f52ec5fe6a673e91b3fa2b829153e04498c19c0fd9b4660f4829ca0579409` | `bf1c9114af4e` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/control-revision.rollback.yaml` | 931 | `20260414T201917Z` | `6a66fd543b97ebcf941d71b039e2c60377089183219371bf5e4dafa130ca1146` | `8f394e5fcb17` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cpar_meta.json` | 22 | `20260414T201917Z` | `37ec4665a8102d115ffd1ac20dae94c98b4dac64b0c1a68228aa2a531caeb35d` | `bfc1a816bc67` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/data_diagnostics.json` | 3001 | `20260414T201917Z` | `2c13646862256fed907b45829520ef98900c3358e62d36dc52e2f5056ceb234e` | `41d16912ee75` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/health_diagnostics.json` | 1806 | `20260414T201917Z` | `52426396789130a768a65a31c993d6b38f33f1ac610ebff03200d83a2503e561` | `12f053dce543` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.forward.yaml` | 2917 | `20260414T201917Z` | `8b82fab5530b7bec04f62f783901b3f47657c90717cfea1c6ba49468776a0796` | `ea95f6b4fde1` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.rollback.yaml` | 2902 | `20260414T201917Z` | `540fe6866abe4d3d379ff094f0426d8ce01c68804f34d75ac3fb28a1936f0149` | `9d72866d9780` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.forward.retry.txt` | 320261 | `20260414T201917Z` | `f6176520ec43277663b0ed7cb718160f6551dbbffcb45f8983fc5399a9362327` | `4ea17daa61cd` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.forward.txt` | 304730 | `20260414T201917Z` | `affef401b1087a88e2ab8d64480adab175caf58a8ef7ae7b076b2c2cb7d8dff3` | `d2edeace87fa` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.rollback.txt` | 320261 | `20260414T201917Z` | `ecd07305c29ca58609fcfb03e0f44d16fe043141d579bbcac566248f312969e7` | `f9f64fd6ce39` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator_status.json` | 109910 | `20260414T201917Z` | `fc81973d862bb8034fa1ad696be9b79a9ff9f35d937d3ade40c3e1a08972b9ac` | `4318a5e6dd02` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/refresh_status.json` | 50107 | `20260414T201917Z` | `e6b699931bf577d9026ea4091b4415e5fbe740adf01b84374fdc41074c32872a` | `ffbf596b71c0` | `05a579c521d0` |
| `phase4_20260414T201917Z/rollout-bundle-capture.txt` | 1100 | `20260414T201917Z` | `ded88e627a3fccbc2c1629a2ad8689221da5275e995ae8e3676e3d1867aaaade` | `abc1f2a909ad` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/service_control.json` | 5623 | `20260414T201917Z` | `5ec7edea3f8c8416b4e29036de105105b02eacb165261e656d5d3a0e24ebb150` | `bc334c9a0728` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/service_frontend.json` | 3720 | `20260414T201917Z` | `a6a71138220b7e386ffbf6195ba93724665220dbbc5b986bdd6ded3a18bc1285` | `06cd30e68a7f` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/service_serve.json` | 4834 | `20260414T201917Z` | `963faa6793e50f2ef8d4de4646667b9c9ee50cbd009326a1b892015afbe3f446` | `18c910a20fe7` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/terraform-output.json` | 8092 | `20260414T201917Z` | `b9e11f76ce749e973ccbd6dbd64817933656b618d04170699694b45921fd1ebf` | `b9512b7dd8ee` | `2cccf0f987ae` |
| `phase4_20260414T201917Z/topology-contract.md` | 818 | `20260414T201917Z` | `1b045a28bf5423a6eb1a40a55ceeb32abc074e8864a1a28ddcd56b468d92af2a` | `88702958fd34` | `2cccf0f987ae` |

## Byte-Identical Groups


1. SHA-256 `e3247db2fa39c2c715f759b20ed09154d16b8017a7a0fc14a9ea94214ad35dc2`; 2 paths; 6015004 bytes each.
   - `phase4_20260414T201917Z/day1/api_health_diagnostics.json`
   - `phase4_20260414T201917Z/day1/control_api_health_diagnostics.json`

2. SHA-256 `e6b699931bf577d9026ea4091b4415e5fbe740adf01b84374fdc41074c32872a`; 5 paths; 50107 bytes each.
   - `phase4_20260414T201917Z/day1/api_refresh_status.json`
   - `phase4_20260414T201917Z/day1/api_refresh_status_post_fix.json`
   - `phase4_20260414T201917Z/day1/api_refresh_status_post_source_daily.json`
   - `phase4_20260414T201917Z/day1/control_api_refresh_status.json`
   - `phase4_20260414T201917Z/rollback_drill/20260414T215254Z/refresh_status.json`

3. SHA-256 `f5262b89e39702e1b336d269e30ce4aeb7025b5853689203f985f094653a69b8`; 2 paths; 11505 bytes each.
   - `phase4_20260414T193820Z/api_refresh_status.json`
   - `phase4_20260414T201917Z/api_refresh_status.json`

4. SHA-256 `7f1d73ce44715b4d562ed3ba4d32aeea8dd910388467aba4ec019e2f1bb4c261`; 2 paths; 8092 bytes each.
   - `phase4_20260414T193820Z/terraform-output.json`
   - `phase4_20260414T201840Z/terraform-output.json`

5. SHA-256 `2bfef01143529b54b36ea56b1fe45c87f71e6f48e4cb0f8be20e2bd9c225ad33`; 3 paths; 3553 bytes each.
   - `phase4_20260414T193820Z/api_cpar_meta.json`
   - `phase4_20260414T201917Z/api_cpar_meta.json`
   - `phase4_20260414T201917Z/day1/api_cpar_meta.json`

6. SHA-256 `963faa6793e50f2ef8d4de4646667b9c9ee50cbd009326a1b892015afbe3f446`; 2 paths; 4834 bytes each.
   - `phase4_20260414T193820Z/service_serve.json`
   - `phase4_20260414T201917Z/service_serve.json`

7. SHA-256 `a6a71138220b7e386ffbf6195ba93724665220dbbc5b986bdd6ded3a18bc1285`; 2 paths; 3720 bytes each.
   - `phase4_20260414T193820Z/service_frontend.json`
   - `phase4_20260414T201917Z/service_frontend.json`

8. SHA-256 `97334a5fbd0293b77fb6a9a73cebffe5c17a30fbffa1fefb6d6e6b27813db92d`; 2 paths; 3184 bytes each.
   - `phase4_20260414T193820Z/api_data_diagnostics.json`
   - `phase4_20260414T201917Z/api_data_diagnostics.json`

9. SHA-256 `cd0c525a726b6c2e6ac02f0f012dddc44323b6ab4a90aa92a27edf95d4dc6fa2`; 2 paths; 3184 bytes each.
   - `phase4_20260414T201917Z/day1/api_data_diagnostics.json`
   - `phase4_20260414T201917Z/day1/control_api_data_diagnostics.json`

10. SHA-256 `ce5a6988f3c1843ed9108cc10ad1cdf519d1db849727b75594edf80ea607c9d0`; 2 paths; 3157 bytes each.
   - `phase4_20260414T193820Z/job_serve_refresh.json`
   - `phase4_20260414T201917Z/job_serve_refresh.json`

11. SHA-256 `50edfa0d39533223cb84e1870bfd5799d85f29faf4e9ccabd9bee11c580a33f8`; 2 paths; 3146 bytes each.
   - `phase4_20260414T193820Z/job_cpar_build.json`
   - `phase4_20260414T201917Z/job_cpar_build.json`

12. SHA-256 `46f1dfa4c5fb356b75005b530cb7a975d1799c59b7c2171aa6ed38df14e3a395`; 2 paths; 3143 bytes each.
   - `phase4_20260414T193820Z/job_core_weekly.json`
   - `phase4_20260414T201917Z/job_core_weekly.json`

13. SHA-256 `066ab1b6b5e08a11895f3caf794781540b1464ef21cce7249b182fa7fbae015d`; 2 paths; 3140 bytes each.
   - `phase4_20260414T193820Z/job_cold_core.json`
   - `phase4_20260414T201917Z/job_cold_core.json`

14. SHA-256 `8a192d40a2f6d955f27717101d4ccb4d7052128fe0582e7b0dd1fe12fe0df297`; 2 paths; 1751 bytes each.
   - `phase4_20260414T193820Z/api_health_diagnostics.json`
   - `phase4_20260414T201917Z/api_health_diagnostics.json`

15. SHA-256 `1b045a28bf5423a6eb1a40a55ceeb32abc074e8864a1a28ddcd56b468d92af2a`; 3 paths; 818 bytes each.
   - `phase4_20260414T193820Z/topology-contract.md`
   - `phase4_20260414T201840Z/topology-contract.md`
   - `phase4_20260414T201917Z/topology-contract.md`

These files remain separate because their paths record different endpoints,
phases, or checkpoints. Any future de-duplication requires records-owner
approval and a durable external archive; Git history alone is not the
retention mechanism.

## Tracked Inbound Reference Graph

| Source | Evidence target |
| --- | --- |
| `docs/archive/implementation-trackers/FRONTEND_AUTH_AND_CUSTOM_DOMAIN_PLAN_2026-04-14.md` | `docs/operations/cutover_evidence/FRONTEND_AUTH_EXECUTION_20260415T010336Z.md` |
| `docs/operations/CLOUD_NATIVE_RUNBOOK.md` | `docs/operations/cutover_evidence/FRONTEND_AUTH_EXECUTION_20260415T010336Z.md` |
| `docs/operations/CLOUD_NATIVE_RUNBOOK.md` | `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` |
| `docs/operations/CLOUD_NATIVE_RUNBOOK.md` | `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` |
| `docs/operations/FULL_CLOUD_COMPUTE_CUTOVER_PLAN.md` | `docs/operations/cutover_evidence/PHASE4_STABILITY_AND_ROLLBACK_TEMPLATE.md` |
| `docs/operations/ROLLBACK_PROCEDURE.md` | `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` |
| `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_operator_status_post_fix.json` |
| `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_refresh_status_post_fix.json` |
| `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/cloud-topology-check.txt` |
| `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/job_core_weekly_pv25b.json` |
| `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/operator-check.txt` |
| `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/source-daily-post-fix.txt` |
| `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/source-daily.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/PHASE4_DAY1_20260414T210100Z.md` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/api_cpar_meta.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/api_data_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/api_health_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/api_operator_status.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/api_refresh_status.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/cloud-topology-check.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/job_cold_core.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/job_core_weekly.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/job_cpar_build.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/job_serve_refresh.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/operator-check.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/rollout-bundle-capture.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/service_control.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/service_frontend.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/service_serve.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/terraform-output.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T193820Z/topology-contract.md` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_cpar_meta.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_data_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_health_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_operator_status.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_operator_status_post_fix.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/api_refresh_status.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/job_core_weekly_pv25b.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/source-daily-post-fix.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/day1/source-daily.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.forward.retry.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.rollback.txt` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.forward.yaml` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.rollback.yaml` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator_status.json` |
| `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/topology-contract.md` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.forward.retry.txt` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.forward.txt` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.rollback.txt` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/control-revision.forward.yaml` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/control-revision.rollback.yaml` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/data_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/health_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.forward.yaml` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.rollback.yaml` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.forward.retry.txt` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.forward.txt` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.rollback.txt` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator_status.json` |
| `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/refresh_status.json` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/api_cpar_meta.json` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/api_data_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/api_health_diagnostics.json` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/api_operator_status.json` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/api_refresh_status.json` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/cloud-topology-check.txt` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/operator-check.txt` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollout-bundle-capture.txt` |
| `docs/operations/cutover_evidence/PHASE4_UPDATE_20260414T201917Z.md` | `docs/operations/cutover_evidence/phase4_20260414T201917Z/service_control.json` |

The graph resolves literal repository paths plus Markdown links and inline
path references. Evidence must not move unless this graph is regenerated and
all affected references are updated atomically.
