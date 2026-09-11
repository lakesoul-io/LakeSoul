# Vector index benchmark report

E1-E4 matrix on GIST1M (960d) and GloVe-200d (200d), 100K base + 10x10K updates, drift strength 3.0, nlist 256, nprobe 64, top-k 10, 16 threads, local FS, vortex data files.

## E2 — fresh build scaling

| dataset | N | nlist | build (ms) | peak RSS (GB) | index (MiB) |
|---|---:|---:|---:|---:|---:|
| gist | 100,000 | 256 | 2319 | 3.29 | 85.8 |
| gist | 100,000 | 1024 | 6847 | 3.37 | 90.4 |
| gist | 300,000 | 256 | 4569 | 5.65 | 254.4 |
| gist | 300,000 | 1024 | 10210 | 5.63 | 259.0 |
| gist | 1,000,000 | 256 | 18151 | 13.39 | 844.5 |
| gist | 1,000,000 | 1024 | 60823 | 14.09 | 849.1 |
| glove | 100,000 | 256 | 655 | 0.43 | 26.0 |
| glove | 100,000 | 1024 | 3142 | 0.51 | 27.3 |
| glove | 100,000 | 4096 | 14805 | 0.94 | 33.1 |
| glove | 300,000 | 256 | 1914 | 0.97 | 77.1 |
| glove | 300,000 | 1024 | 4857 | 0.99 | 78.4 |
| glove | 300,000 | 4096 | 28530 | 1.49 | 83.8 |
| glove | 1,000,000 | 256 | 4172 | 2.62 | 256.0 |
| glove | 1,000,000 | 1024 | 8763 | 2.65 | 257.4 |
| glove | 1,000,000 | 4096 | 36000 | 2.65 | 262.7 |

## E1 — policy x drift (stream, recall@10 checkpoint queries follow the drift)


### glove

| drift | policy | min recall | final recall | rebuilds | index update (ms) |
|---|---|---:|---:|---:|---:|
| uniform | always | 0.881 | 0.890 | 5 | 5331 |
| uniform | auto@0.25 | 0.881 | 0.890 | 3 | 3412 |
| uniform | auto@0.5 | 0.885 | 0.890 | 2 | 3009 |
| uniform | auto@1.0 | 0.880 | 0.895 | 1 | 2233 |
| uniform | auto@2.0 | 0.880 | 0.908 | 0 | 1428 |
| uniform | none | 0.880 | 0.908 | 0 | 1316 |
| uniform | periodic | 0.881 | 0.895 | 3 | 3686 |
| skew | always | 0.740 | 0.745 | 5 | 4992 |
| skew | auto@0.25 | 0.708 | 0.708 | 4 | 4297 |
| skew | auto@0.5 | 0.666 | 0.666 | 3 | 3542 |
| skew | auto@1.0 | 0.604 | 0.604 | 2 | 2643 |
| skew | auto@2.0 | 0.613 | 0.613 | 1 | 1969 |
| skew | none | 0.741 | 0.741 | 0 | 1445 |
| skew | periodic | 0.708 | 0.708 | 3 | 3613 |
| shift | always | 0.890 | 0.910 | 5 | 5027 |
| shift | auto@0.25 | 0.890 | 0.903 | 4 | 4036 |
| shift | auto@0.5 | 0.890 | 0.932 | 3 | 3404 |
| shift | auto@1.0 | 0.890 | 0.935 | 2 | 2515 |
| shift | auto@2.0 | 0.890 | 0.932 | 2 | 2367 |
| shift | none | 0.868 | 0.884 | 0 | 1138 |
| shift | periodic | 0.890 | 0.903 | 3 | 3408 |

### gist

| drift | policy | min recall | final recall | rebuilds | index update (ms) |
|---|---|---:|---:|---:|---:|
| uniform | always | 0.969 | 0.979 | 5 | 14869 |
| uniform | auto@1.0 | 0.966 | 0.980 | 1 | 6071 |
| uniform | none | 0.966 | 0.977 | 0 | 3267 |
| uniform | periodic | 0.962 | 0.972 | 3 | 10889 |
| skew | always | 0.897 | 0.897 | 5 | 15385 |
| skew | auto@1.0 | 0.897 | 0.912 | 2 | 8171 |
| skew | none | 0.903 | 0.914 | 0 | 3246 |
| skew | periodic | 0.902 | 0.902 | 3 | 9955 |
| shift | always | 0.927 | 0.938 | 5 | 15458 |
| shift | auto@1.0 | 0.929 | 0.938 | 3 | 10153 |
| shift | none | 0.769 | 0.769 | 0 | 3623 |
| shift | periodic | 0.786 | 0.933 | 3 | 10708 |

## E3 — per-cluster vs shard-level trigger

| dataset | drift | threshold | cluster first round | cluster recall | shard first round | shard recall |
|---|---|---:|---:|---:|---:|---:|
| gist | shift | 0.25 | 1 | 0.813 | 3 | 0.811 |
| gist | shift | 0.5 | 1 | 0.813 | 6 | 0.793 |
| gist | shift | 1.0 | 1 | 0.813 | - | - |
| gist | shift | 2.0 | 1 | 0.813 | - | - |
| gist | skew | 0.25 | 1 | 0.971 | 3 | 0.965 |
| gist | skew | 0.5 | 1 | 0.971 | 6 | 0.921 |
| gist | skew | 1.0 | 2 | 0.968 | - | - |
| gist | skew | 2.0 | 3 | 0.965 | - | - |
| gist | uniform | 0.25 | 1 | 0.968 | 3 | 0.979 |
| gist | uniform | 0.5 | 2 | 0.973 | 6 | 0.982 |
| gist | uniform | 1.0 | 6 | 0.982 | - | - |
| gist | uniform | 2.0 | - | - | - | - |
| glove | shift | 0.25 | 1 | 0.890 | 3 | 0.883 |
| glove | shift | 0.5 | 1 | 0.890 | 6 | 0.876 |
| glove | shift | 1.0 | 1 | 0.890 | - | - |
| glove | shift | 2.0 | 1 | 0.890 | - | - |
| glove | skew | 0.25 | 1 | 0.917 | 3 | 0.878 |
| glove | skew | 0.5 | 1 | 0.917 | 6 | 0.855 |
| glove | skew | 1.0 | 1 | 0.917 | - | - |
| glove | skew | 2.0 | 2 | 0.911 | - | - |
| glove | uniform | 0.25 | 2 | 0.901 | 3 | 0.899 |
| glove | uniform | 0.5 | 4 | 0.901 | 6 | 0.880 |
| glove | uniform | 1.0 | 8 | 0.890 | - | - |
| glove | uniform | 2.0 | - | - | - | - |

## E4 — search on fresh / delta / rebuilt index states

| dataset | state | load (ms) | best recall@10 | at nprobe | QPS |
|---|---|---:|---:|---:|---:|
| glove | fresh | 249 | 0.970 | 256 | 14621 |
| glove | delta | 312 | 0.949 | 256 | 13783 |
| glove | rebuilt | 260 | 0.951 | 256 | 13928 |
| gist | fresh | 687 | 0.977 | 128 | 13334 |
| gist | delta | 992 | 0.971 | 64 | 15088 |
| gist | rebuilt | 934 | 0.971 | 128 | 14339 |

## Plots

- `plots/e1_quality_cost_gist.png`
- `plots/e1_quality_cost_glove.png`
- `plots/e1_recall_gist_shift.png`
- `plots/e1_recall_gist_skew.png`
- `plots/e1_recall_gist_uniform.png`
- `plots/e1_recall_glove_shift.png`
- `plots/e1_recall_glove_skew.png`
- `plots/e1_recall_glove_uniform.png`
- `plots/e2_build_gist.png`
- `plots/e2_build_glove.png`
- `plots/e3_trigger_gist.png`
- `plots/e3_trigger_glove.png`
- `plots/e4_recall_qps_gist.png`
- `plots/e4_recall_qps_glove.png`
