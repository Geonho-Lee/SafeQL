-- bootstrap begin

CREATE OR REPLACE FUNCTION chat(
    context TEXT,
    query_string TEXT
) RETURNS SETOF record
AS $$
BEGIN
    RETURN query
    SELECT 1;
END; $$ 
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION softql(
    query_string TEXT
) RETURNS SETOF record
AS $$
BEGIN
    RETURN query
    SELECT 1;
END; $$ 
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION safeql(
    query_string TEXT
) RETURNS SETOF record
AS $$
BEGIN
    RETURN query
    SELECT 1;
END; $$ 
LANGUAGE plpgsql
PARALLEL UNSAFE;

-- List of shell types

CREATE TYPE vector;
CREATE TYPE vecf16;
CREATE TYPE svector;
CREATE TYPE bvector;

CREATE TYPE vector_index_stat;

CREATE TYPE sphere_vector;
CREATE TYPE sphere_vecf16;
CREATE TYPE sphere_svector;
CREATE TYPE sphere_bvector;

-- bootstrap end
