# pyeng-primitives

A collection of small, framework-quality Python classes that model the behaviours and invariants commonly found in modern data engineering pipelines.

This repository is my personal lab for learning how to design clean, correct, and composable Python abstractions using the Python Data Model:

- `__getitem__`
- `__iter__`
- `__len__`
- `__contains__`
- `__add__`
- slicing semantics
- immutability rules
- schema enforcement
- invariant preservation

The goal is to practise writing Python **the way real data platforms do** (Pandas, Polars, Spark, Flink).

Each primitive is fully tested with `pytest`.

---

## Included Primitives

### **✔ ShardBatch**
A batch of events that all belong to the **same shard**.

- enforces single-shard invariant  
- enforces stable schema  
- stable slicing (empty slices preserve shard & schema)  
- supports merging batches and raw row lists  
- tests for schema mismatch, shard mismatch, column access, empty slices, etc.

---

## Testing

All components include accompanying `pytest` test suites.

Run tests via:

```bash
pytest -q
```

## AI Assistance Notice

Some comments/docstrings were refined using generative AI. 
All code logic is authored and reviewed manually.
