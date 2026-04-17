# Enhancing SIEM Systems Using Time Correlation

## Project Overview
This project implements a prototype **pre-correlation pipeline** for SIEM-like authentication events using the **LANL dataset**.

The goal is to reduce low-value and redundant events before downstream correlation, so that the resulting event stream becomes more compact and more meaningful for later analysis.

## Dataset
The current prototype focuses on:
- **LANL authentication events (`auth.txt`)**
- **LANL redteam events (`redteam.txt`)**

Small aligned chunks were extracted from `auth.txt` for controlled experiments.

## Current Pipeline
The implemented pipeline is:

1. Parsing  
2. Normalization  
3. Improved Filtering  
4. Exact De-duplication  
5. Relaxed Temporal Aggregation  
6. Baseline Correlation  
7. Before/After Evaluation  

## Chosen Design Decisions

### Filtering
The selected filter removes:
- `ScreenLock_Success`
- `AuthMap_Success`
- rows where `src_user` starts with `ANONYMOUS LOGON`

### Aggregation
The selected aggregation is **relaxed temporal aggregation** using:
- `src_user`
- `dst_computer`
- `event_type`

### Time Window
Chosen time window:
- **5**

## Main Experimental Findings

### Main Chunk Result
On the main LANL authentication chunk:
- Input auth events: **10000**
- After filtering: **9519**
- After relaxed temporal aggregation: **9439**
- Aggregation reduction: **0.84%**
- Correlation before aggregation: **360**
- Correlation after aggregation: **337**
- Correlation change: **-6.39%**

### Multi-Chunk Result
Across 3 authentication chunks:
- Average event reduction: **0.74%**
- Average correlation reduction: **-4.56%**

## Redteam Relevance
The selected authentication sample overlaps with the LANL redteam data in:
- time range
- users
- computers

A direct row-level match was observed at timestamp **150885**, which supports the relevance of the selected sample for exploratory pre-correlation analysis.

## Project Structure

### Core pipeline
- `lanl_parser.py`
- `lanl_normalizer.py`
- `lanl_filter.py`
- `lanl_deduplicate.py`
- `lanl_temporal_aggregate.py`
- `lanl_baseline_correlation.py`
- `run_lanl_pipeline.py`

### Experiment and evaluation
- `run_multi_chunk_experiment.py`
- `summarize_multi_chunk_experiment.py`
- `final_experiment_decision_summary.py`
- `summarize_redteam_overlap.py`
- `build_master_summary.py`

### Supporting folders
- `experiments/`
- `tests/`
- `outputs/`
- `data/`
- `notebooks/`

## Important Output Files
Generated summaries are saved in:
- `outputs/pipeline_summary.txt`
- `outputs/multi_chunk_experiment_summary.txt`
- `outputs/final_experiment_decision_summary.txt`
- `outputs/redteam_overlap_summary.txt`
- `outputs/master_project_summary.txt`

## Current Status
The prototype is working and produces repeated evidence that pre-correlation processing can reduce authentication event streams before downstream correlation.