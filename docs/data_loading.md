# Data Loading

SafeQL builds on top of PostgreSQL.  
Data loading for SafeQL follows a workflow similar to standard PostgreSQL usage, with additional steps for configuring vector embeddings.

This page describes:

- How to enable the vector extension  
- How to configure embedding backends (fastembed / openai)  
- Supported embedding models  
- How to load vector metadata so SafeQL can use embeddings during refinement  

---

## 1. Enabling the Vector Extension

SafeQL requires the `vectors` extension to manage embeddings and perform similarity search.

Enable the extension:

```sql
CREATE EXTENSION vectors;
LOAD 'vectors';
```

Set vector-related search path (recommended):

```sql
SET search_path TO "$user", public, vectors;
```

---

## 2. Choosing an Embedding Backend

pgvecto.rs supports **two embedding backends**:

- **fastembed** (local, CPU/GPU acceleration)
- **openai** (remote API)

You can switch between backends at any time using:

```sql
SET vectors.embedding_backend TO 'fastembed';
```

or:

```sql
SET vectors.embedding_backend TO 'openai';
```

---

## 3. FastEmbed Backend

FastEmbed is a lightweight, high-performance embedding backend supporting both **CPU and GPU acceleration** (CUDA).

Enable:

```sql
SET vectors.embedding_backend TO 'fastembed';
```

Select a model (example):

```sql
SET vectors.embedding_model_name TO 'Xenova/bge-base-en-v1.5';
```

GPU acceleration will automatically be used if a supported device is available.

### Supported FastEmbed Models

- Xeonova/all-MiniLM-L6-v2  
- Xenova/bge-base-en  
- Xenova/bge-base-en-v1.5  
- Xenova/bge-small-en  
- Xenova/bge-large-en  
- intfloat/e5-base  
- intfloat/e5-large  
- More models available depending on system installation  

---

## 4. OpenAI Backend

If you prefer remote embedding services, enable the OpenAI backend:

```sql
SET vectors.embedding_backend TO 'openai';
```

You must provide:

### 4.1 API key

```sql
SET vectors.openai_api_key TO 'YOUR_API_KEY';
```

### 4.2 Optional: Custom Base URL  
(For self-hosted or proxy OpenAI APIs)

```sql
SET vectors.openai_base_url TO 'https://api.openai.com/v1';
```

### 4.3 OpenAI Embedding Model

```sql
SET vectors.embedding_model_name TO 'text-embedding-3-large';
```

### Supported OpenAI models

- `text-embedding-3-small`
- `text-embedding-3-large`
- And all models supported by your OpenAI-compatible API endpoint

---

## 5. Loading Vector Metadata for SafeQL

SafeQL uses embedding-based similarity for:

- Refinement ranking  
- Pruning  
- Table/column relevance evaluation  

To use these features, SafeQL needs to load schema-level and value-level embedding tables.

Run the following initialization functions:

### 5.1 Load vector tables

```sql
SELECT load_vector_tables();
```

Loads metadata for all tables in the current database.

---

### 5.2 Load vector fields

```sql
SELECT load_vector_fields();
```

Loads schema information for columns.

---

### 5.3 Load vector operators

```sql
SELECT load_vector_operators();
```

Loads available operators in SQL.

---

### 5.4 Load vector functions

```sql
SELECT load_vector_functions();
```

Loads available functions in SQL.

---

### 5.5 Load vector values

```sql
SELECT load_vector_values();
```

Embeds actual **data values** (varchar, text, etc.) so SafeQL can use embedding similarity for refinement.

---

## Summary

SafeQL integrates deeply with vector embeddings.  
To use refinement correctly:

1. Enable the vectors extension  
2. Choose an embedding backend  
3. Configure the embedding model  
4. Load vector metadata  

Once finished, SafeQL can use embeddings for semantic distance search and pruning optimizations.

