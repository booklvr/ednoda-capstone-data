# Future Dataset Candidates (Research Backlog)

## 1. Why this file exists

- v1 is intentionally limited to CEFR-SP, CEFR-J, and the optional Ednoda snapshot.
- Additional datasets are being considered for a v2 expansion after the local pipeline is validated on real source files.
- This document is a backlog note for research planning, not an implementation commitment.

## 2. Strong candidates to investigate after v1

### Ace-CEFR
- **What it is:** CEFR-tagged learner-focused sentence corpus resources.
- **Why it may help Ednoda:** Could expand CEFR-aligned sentence coverage for recommendation candidates.
- **Likely role:** sentence pool; evaluation resource.
- **Caution:** Check licensing and sentence quality consistency before ingestion.

### Tatoeba
- **What it is:** Large multilingual sentence collection with broad domain coverage.
- **Why it may help Ednoda:** Large candidate pool for ESL sentence retrieval and filtering.
- **Likely role:** sentence pool.
- **Caution:** Requires strong quality filtering and licensing review for redistribution constraints.

### CWI 2018
- **What it is:** Complex Word Identification shared-task data.
- **Why it may help Ednoda:** Useful signal for lexical difficulty and simplification-aware ranking.
- **Likely role:** lexical difficulty supervision; evaluation resource.
- **Caution:** Annotation scope may not map cleanly to sentence-level recommendation without adaptation.

### CompLex 2.0
- **What it is:** Lexical complexity prediction dataset with graded complexity labels.
- **Why it may help Ednoda:** Can strengthen word-level difficulty features for ESL personalization.
- **Likely role:** lexical difficulty supervision.
- **Caution:** Domain and annotation assumptions may require calibration for K-12 ESL use.

## 3. Possible later additions

### OneStopEnglish
- **Why interesting:** Graded reading materials are directly relevant to CEFR progression.
- **Why not in v1:** Not needed to validate core v1 ingestion/merge/validation workflow.
- **Main blocker:** Licensing and reuse restrictions.

### SCoRE
- **Why interesting:** Potentially useful for short-turn readability/difficulty signals.
- **Why not in v1:** Pipeline currently centered on robust source onboarding and schema stability.
- **Main blocker:** Filtering and task-fit effort for Ednoda atomic item workflow.

### ASSET
- **Why interesting:** Valuable simplification benchmark for downstream evaluation ideas.
- **Why not in v1:** Not required for initial data foundation validation.
- **Main blocker:** Primarily evaluation-oriented rather than direct retrieval-pool expansion.

## 4. Likely out of scope / low priority for this capstone

### CLEAR
- Weak fit for immediate Ednoda goals: likely adds overhead without direct benefit to atomic ESL item retrieval.

### British Council content scraping/reuse
- Weak fit due to legal and operational risk: scraping/reuse constraints make it unsuitable for a stable capstone pipeline.

### Learner essay corpora as retrieval pools
- Weak fit for atomic ESL-item workflow: long-form learner essays are noisy as recommendation pool units and require heavy segmentation/filtering.
