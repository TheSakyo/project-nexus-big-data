# Project defense outline — 10 minutes per student

> This is a planning outline. Replace placeholders with verified findings after processing the real data. Adjust the schedule if the group has multiple students.

| Suggested time | Section | Expected content |
|---:|---|---|
| 0:00–1:00 | Context and business need | Introduce Nexus Interactive, the decision to support, and the main question. |
| 1:00–2:00 | Sources | Show sources actually received, formats, periods, sizes, and limitations. |
| 2:00–3:15 | Architecture | Explain ingestion and `RAW`, `PROCESSED`, `ANALYTICS` zones with a clear diagram. |
| 3:15–4:15 | Data quality | Show checks performed and records kept, corrected, or rejected. |
| 4:15–5:45 | Hadoop jobs | Describe Java MapReduce jobs, their cross-source join, and restart behavior. |
| 5:45–7:30 | Findings | Present DAU/WAU/retention, the business track, and open-ended exploration with definitions and limitations. |
| 7:30–8:45 | Scalability | Give the measured volume, heap limit, duration, tasks, and Hadoop counters. |
| 8:45–9:30 | Recommendations and limits | Tie recommendations to verified findings; distinguish correlation from causality. |
| 9:30–10:00 | Conclusion | Summarize the answer to the business question and next steps. |

## Visuals to prepare

1. Map of sources and observed data volumes.
2. Data lake diagram and data lineage.
3. Mapper → Shuffle/Sort → Reducer flow for each job.
4. Metrics chart with period, population, and denominator shown.
5. Table of data-quality checks and counters.
6. Diagram or table of scalability measurements.

## Defense checklist

- Every number is reproducible from a command, job, and saved output.
- Synthetic data is clearly labeled and separated from real findings.
- Activity, retention, and join rules are explicit.
- Data, method, and infrastructure limitations are visible.
- Each student's speaking time has been rehearsed against the planned schedule.
