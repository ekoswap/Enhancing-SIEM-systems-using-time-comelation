# Enhancing SIEM Systems Using Time Correlation

## Overview
This repository contains the implementation of a **temporal pre-correlation prototype** for improving SIEM-like event streams before baseline correlation.

The project focuses on reducing low-value, repetitive, and temporally fragmented events before downstream analysis. The implementation uses the **LANL Cyber Security Dataset** and adopts a **source-aware** design:
- **AUTH** is the main end-to-end path.
- **PROC**, **FLOWS**, and **DNS** are exploratory extensions.

## Main Idea
Instead of sending raw security events directly to correlation, the project applies a pre-correlation layer that:
1. Parses raw logs into structured events.
2. Normalizes temporal and semantic fields.
3. Removes low-value events.
4. Removes exact duplicates.
5. Aggregates temporally related events.
6. Measures the effect before and after baseline correlation.

## Dataset
The practical implementation is based on the **LANL Comprehensive, Multi-Source Cyber-Security Events** dataset.

### Main data sources used
- `AUTH` — authentication events (**main path**)
- `PROC` — process lifecycle events (**exploratory**)
- `FLOWS` — network flow records (**exploratory**)
- `DNS` — DNS communication records (**exploratory**)
- `redteam` — validation reference

## Final AUTH Pipeline
The selected AUTH pipeline is:

1. **Parsing**
2. **Normalization**
3. **Improved Filtering**
4. **Exact De-duplication**
5. **Relaxed Temporal Aggregation**
6. **Baseline Correlation**
7. **Before/After Evaluation**

### Selected AUTH design decisions
- **Filtering removes**
  - `ScreenLock_Success`
  - `AuthMap_Success`
  - rows where `src_user` starts with `ANONYMOUS LOGON`
- **Aggregation key**
  - `src_user`
  - `dst_computer`
  - `event_type`
- **Time window**
  - `5`

## Main Results
### AUTH main path
- Input AUTH events: **10000**
- After filtering: **9519**
- After relaxed temporal aggregation: **9439**
- Correlation outputs before aggregation: **360**
- Correlation outputs after aggregation: **337**
- Correlation reduction: **23 outputs (6.39%)**
- Redteam-related context: **73.15% → 72.67%**

### Validation
- **3-chunk validation** for AUTH
- **8 redteam-zone chunks** for AUTH stability checking

### Cross-source exploratory results
| Source | Baseline % | Aggregated % | Interpretation |
|---|---:|---:|---|
| AUTH | 73.15% | 72.67% | Main path; near-stable preservation with measurable downstream reduction |
| DNS | 62.55% | 64.71% | Communication-pair aggregation preserved a strong signal |
| FLOWS | 39.14% | 42.87% | Duration-based reduction improved concentration |
| PROC | 10.63% | 20.65% | End-only strategy sharply improved concentration |

## Repository Structure
### Main folders
- `experiments/`
- `notebooks/`
- `outputs/`
- `pipeline/`
- `tests/`

### Main scripts
- `lanl_parser.py`
- `lanl_normalizer.py`
- `lanl_filter.py`
- `lanl_deduplicate.py`
- `lanl_temporal_aggregate.py`
- `lanl_baseline_correlation.py`
- `run_lanl_pipeline.py`
- `run_multi_chunk_experiment.py`
- `build_master_summary.py`
- `build_data_source_comparison.py`
- `final_experiment_decision_summary.py`
- `make_result_charts.py`

### Modular pipeline files
Inside `pipeline/`, the final modular structure is centered around:
- `common.py`
- `source_registry.py`
- `generic_runner.py`
- source-specific strategies for AUTH / PROC / FLOWS / DNS

## Outputs
Generated summaries and comparison outputs are stored in `outputs/`.

Important repository outputs include:
- pipeline summaries
- multi-chunk summaries
- source comparison outputs
- result charts used for the final defense

## How to Run
> Adjust paths and input files according to your local LANL dataset setup.

Typical workflow:
```bash
python run_lanl_pipeline.py
python run_multi_chunk_experiment.py
python build_master_summary.py
python build_data_source_comparison.py
python make_result_charts.py
```

## Requirements
Recommended environment:
- Python 3.12
- pandas
- matplotlib
- jupyter
- python-dateutil

Example installation:
```bash
pip install pandas matplotlib jupyter python-dateutil
```

## Report
The final report is included in this repository as:
- `التقرير النهائي .pdf`

## Project Status
The repository represents a working graduation-project prototype that demonstrates:
- improving the stream before correlation is **feasible**
- the effect is **measurable**
- the framework is **unified**
- implementation decisions must remain **source-aware**

## Future Work
Possible future directions:
- connect the framework to a more realistic correlation engine
- test additional datasets
- study adaptive temporal windows
- explore real-time streaming architecture
- consider ML only after stabilizing the interpretable pre-correlation layer
