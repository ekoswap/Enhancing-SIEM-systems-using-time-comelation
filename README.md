# Enhancing SIEM Systems Using Time Correlation

## Project Overview

This graduation project focuses on enhancing Security Information and Event Management (SIEM) systems using time correlation.

The implemented work introduces a temporal pre-correlation layer before baseline correlation. The goal is to reduce redundancy, noise, and repeated events before the event stream reaches the correlation stage.

The project is implemented as a Python prototype using the LANL Cyber Security Dataset. AUTH is the main implementation path, while PROC, FLOWS, and DNS are treated as exploratory source-aware extensions.

---

## Main Idea

Modern SIEM systems receive large volumes of heterogeneous and time-stamped security events. Sending raw events directly to correlation may increase redundancy, alert noise, and downstream processing load.

This project addresses that problem by applying a pre-correlation pipeline that includes:

1. Parsing
2. Normalization
3. Filtering
4. De-duplication
5. Temporal Aggregation
6. Baseline Correlation
7. Evaluation

The main objective is to improve the event stream before correlation while preserving attack-related context as much as possible.

---

## Project Scope

This project is a local Python prototype, not a full production SIEM system.

The implementation focuses on:

- Building an interpretable pre-correlation pipeline
- Using time as an active factor in event grouping
- Reducing redundant and low-value events
- Measuring before/after impact on baseline correlation
- Evaluating attack-related context preservation
- Extending the idea across multiple data sources using source-aware strategies

The project does not use machine learning.

---

## Dataset

The project uses the LANL Cyber Security Dataset.

Used sources:

- AUTH: main implementation path
- PROC: exploratory extension
- FLOWS: exploratory extension
- DNS: exploratory extension
- Redteam: validation reference

The raw dataset files are not uploaded to GitHub because of their large size.

Expected local data structure:

```text
data/
  raw/
    auth_sample_late.txt
    redteam.txt
    proc.txt
    flows.txt
    dns.txt
```

---

## Repository Structure

```text
.
├── README.md
├── .gitignore
├── requirements.txt
├── التقرير النهائي .pdf
│
├── lanl_parser.py
├── lanl_normalizer.py
├── lanl_filter.py
├── lanl_deduplicate.py
├── lanl_temporal_aggregate.py
├── lanl_baseline_correlation.py
├── run_lanl_pipeline.py
│
├── pipeline/
│   ├── __init__.py
│   ├── auth_preparation.py
│   ├── auth_strategy.py
│   ├── common.py
│   ├── dns_strategy.py
│   ├── flows_strategy.py
│   ├── generic_runner.py
│   ├── proc_strategy.py
│   ├── save_runner_results.py
│   └── source_registry.py
│
├── outputs/
├── docs/
└── data/
```

---

## Main Code Components

### Core LANL Pipeline Files

These files represent the main processing stages of the project:

- `lanl_parser.py`  
  Parses raw LANL authentication logs into structured events.

- `lanl_normalizer.py`  
  Normalizes event fields so they become suitable for comparison and grouping.

- `lanl_filter.py`  
  Applies filtering rules to reduce low-value events.

- `lanl_deduplicate.py`  
  Removes exact duplicate events.

- `lanl_temporal_aggregate.py`  
  Groups similar and temporally close events using a time-aware aggregation logic.

- `lanl_baseline_correlation.py`  
  Applies a simple baseline correlation mechanism for before/after comparison.

- `run_lanl_pipeline.py`  
  Runs the main AUTH pipeline from raw input to evaluation output.

---

## Final Modular Pipeline

The `pipeline/` folder contains the final source-aware modular implementation.

Main files:

- `pipeline/generic_runner.py`  
  Unified runner for executing the pipeline across supported sources.

- `pipeline/source_registry.py`  
  Defines source-specific properties and configuration.

- `pipeline/common.py`  
  Contains shared functions used by different source strategies.

- `pipeline/auth_strategy.py`  
  Implements the AUTH source strategy.

- `pipeline/proc_strategy.py`  
  Implements the PROC exploratory strategy.

- `pipeline/flows_strategy.py`  
  Implements the FLOWS exploratory strategy.

- `pipeline/dns_strategy.py`  
  Implements the DNS exploratory strategy.

- `pipeline/save_runner_results.py`  
  Saves final runner outputs into structured result files.

---

## How to Run

### 1. Create and activate a virtual environment

On Windows CMD:

```cmd
python -m venv .venv
.venv\Scripts\activate
```

### 2. Install requirements

```cmd
pip install -r requirements.txt
```

### 3. Run the main AUTH pipeline

```cmd
python run_lanl_pipeline.py
```

### 4. Run the final modular multi-source pipeline

```cmd
python pipeline/generic_runner.py
```

---

## Main Results

The main AUTH path achieved the following before/after results:

| Metric | Before | After | Interpretation |
|---|---:|---:|---|
| Event stream size | 10000 | 9439 | The stream was reduced before correlation |
| Baseline correlation outputs | 360 | 337 | Downstream correlation load decreased |
| Redteam-related context | 73.15% | 72.67% | Attack-related context remained near-stable |

The reduction in baseline correlation outputs was:

```text
360 -> 337
Reduction = 23 outputs
Reduction percentage = 6.39%
```

---

## Exploratory Source Results

| Source | Baseline % | Aggregated % | Interpretation |
|---|---:|---:|---|
| AUTH | 73.15% | 72.67% | Main path; near-stable preservation with measurable downstream reduction |
| DNS | 62.55% | 64.71% | Communication-pair aggregation preserved a strong signal |
| FLOWS | 39.14% | 42.87% | Duration-based reduction improved concentration |
| PROC | 10.63% | 20.65% | End-only strategy improved concentration |

These results show that the framework can remain unified, but the strategy must remain source-aware.

---

## Design Decisions

The final implementation follows these decisions:

- AUTH is the main path.
- PROC, FLOWS, and DNS are exploratory extensions.
- The project uses a Python prototype instead of a full SIEM platform.
- The project uses baseline correlation as a reference layer.
- The project evaluates reduction using before/after comparison.
- The project focuses on time-aware pre-correlation without machine learning.
- The project keeps strategies source-aware instead of applying one rule to all sources.

---

## Scientific Contribution

The contribution of this project is not building a complete SIEM system.

The contribution is building and evaluating a temporal pre-correlation layer that improves the security event stream before baseline correlation.

The project shows that stream reduction before correlation can be:

- feasible
- measurable
- interpretable
- source-aware
- connected to downstream correlation impact
- evaluated with redteam-related context preservation

---

## Limitations

The current version has the following limitations:

- It is a local prototype, not a production SIEM deployment.
- It uses selected samples and chunks instead of processing the entire dataset at once.
- The baseline correlation layer is simple and used mainly for comparison.
- AUTH is the most complete path, while PROC, FLOWS, and DNS are exploratory.
- Real-time streaming is left for future work.

---

## Future Work

Possible future improvements include:

- Connecting the pipeline to a more realistic correlation engine
- Testing additional datasets beyond LANL
- Studying adaptive temporal windows
- Extending the prototype into a streaming architecture
- Considering machine learning only after stabilizing the interpretable pre-correlation layer

---

## Author

Prepared by:

**Tala Almasalmeh**

Supervised by:

**Dr. Wassim Junidi**

Syrian Private University  
Faculty of Artificial Intelligence Engineering  
Department of Intelligent Information Security Systems  
Academic Year: 2025–2026