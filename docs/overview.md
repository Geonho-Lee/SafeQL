# Overview

An introduction to **SafeQL**, a search-based refinement framework for correcting LLM-generated SQL queries inside a DBMS.

---

## What is SafeQL?

SafeQL is a **DBMS-integrated refinement engine** that corrects erroneous SQL queries produced by large language models.  
Instead of regenerating the entire query on every failure, SafeQL:

- Interprets DBMS error messages  
- Identifies only the faulty components  
- Generates minimal refinements  
- Validates each candidate through real DBMS execution  
- Guarantees convergence to an executable SQL query  

This design provides:

- **Higher correctness** (avoids repeated hallucinated regeneration)
- **Lower token cost & latency** (no full regeneration loops)

👉 Full details are in the paper.

---

## Why SafeQL?


<p align="center">
<img src="./_static/Figure.svg" width="70%">
</p>

### (a) Regeneration-based refinement  
Traditional Text-to-SQL systems follow a regeneration loop: the model generates an initial query $q_0$, the DBMS executes it, the query fails, and the system discards it entirely before producing a new query $q_1$ using the error message. This process repeats—
$q_2, q_3, ... $—until some executable query eventually appears. Because every iteration rebuilds the whole query, valid fragments are repeatedly thrown away, leading to redundant work and unstable convergence.

### (b) Search-based refinement (*Ours*)
SafeQL takes a fundamentally different approach. When $q_0$ fails, it keeps the original structure intact and uses DBMS feedback to pinpoint the exact faulty component—whether a relation, attribute, function, or value. It then applies a minimal, structure-preserving correction, producing refined queries that reuse all valid parts of the original input. Through this process, SafeQL replaces the traditional trial-and-error regeneration loop with a guided search over a safe query space in which all candidates remain syntactically sound and semantically grounded. Among the possible refinements, it chooses the one that remains closest in meaning to the user’s original intent.



