#ifndef PG_QUERY_READFUNCS_H
#define PG_QUERY_READFUNCS_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

RawStmt* handle_parsed_result(const uint8_t* buf, size_t len);
#endif
