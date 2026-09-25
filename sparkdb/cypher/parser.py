"""Cypher Parser for SparkDB.

Parses OpenCypher queries into structured Abstract Syntax Trees (AST):
  - CREATE (nodes and relationships with properties)
  - MATCH ... WHERE ... RETURN (multi-hop patterns, variable paths, projections)
  - ORDER BY, SKIP, LIMIT
  - MERGE (upsert node patterns with ON CREATE / ON MATCH)
  - CALL procedures (algo.*, db.*, db.idx.*)
  - INDEX creation and deletion (CREATE/DROP INDEX, CREATE/DROP VECTOR INDEX)
  - EXPLAIN / PROFILE execution plan introspection
  - DELETE / DETACH DELETE
  - Parameter substitution ($param, {param})
"""
from __future__ import annotations

import ast
from collections import OrderedDict
import copy
import json
import re
import threading
from typing import Any, Dict, List, Optional, Tuple


def substitute_params(query: str, params: Optional[Dict[str, Any]] = None) -> str:
    """Substitute $param and {param} placeholders with properly formatted literals."""
    if not params:
        return query

    q = query
    for key, val in params.items():
        if isinstance(val, str):
            escaped = val.replace("'", "\\'")
            rep = f"'{escaped}'"
        elif isinstance(val, bool):
            rep = "true" if val else "false"
        elif val is None:
            rep = "null"
        elif isinstance(val, (int, float)):
            rep = str(val)
        elif isinstance(val, (list, dict)):
            rep = json.dumps(val)
        else:
            rep = repr(val)

        # Replace $key (word boundary) and {key}
        q = re.sub(rf"\${re.escape(key)}\b", rep, q)
        q = re.sub(rf"\{{\s*{re.escape(key)}\s*\}}", rep, q)
    return q


class CypherStatement:
    def __init__(self, stmt_type: str, details: Dict[str, Any]):
        self.type = stmt_type
        self.details = details

    def __repr__(self):
        return f"CypherStatement(type={self.type}, details={self.details})"

    def clone(self) -> CypherStatement:
        return CypherStatement(self.type, copy.deepcopy(self.details))


class LRUCache:
    """Thread-safe Least Recently Used (LRU) Cache with fixed capacity."""

    def __init__(self, capacity: int = 2048):
        self.capacity = max(capacity, 2000)
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]
            self._misses += 1
            return default

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            if len(self._cache) > self.capacity:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._cache

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]
            self._misses += 1
            raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "size": len(self._cache),
                "capacity": self.capacity,
                "hits": self._hits,
                "misses": self._misses,
            }


_COMPILED_PLAN_CACHE: LRUCache = LRUCache(capacity=2048)
_TEMPLATE_PLAN_CACHE: LRUCache = LRUCache(capacity=2048)
_PARSED_CACHE: LRUCache = _COMPILED_PLAN_CACHE


class CypherParser:
    """Parses Cypher query strings into executable statement representations."""

    @staticmethod
    def parse_properties(prop_str: str) -> Dict[str, Any]:
        """Parse Cypher property map: {name: 'AMF', domain: '3GPP_CORE'}."""
        prop_str = prop_str.strip()
        if not prop_str or prop_str == "{}":
            return {}

        # Handle vecf32([1, 2, 3]) syntax
        vec_match = re.search(r"vecf32\s*\(\s*(\[[^\]]+\])\s*\)", prop_str)
        if vec_match:
            vec_arr = json.loads(vec_match.group(1))
            prop_str = prop_str.replace(vec_match.group(0), json.dumps(vec_arr))

        # Convert single quotes to double quotes for JSON parsing
        # Normalize {name: 'val'} to {"name": "val"}
        norm = prop_str
        norm = re.sub(r"([{,]\s*)([a-zA-Z_]\w*)\s*:", r'\1"\2":', norm)
        norm = re.sub(r":\s*'([^']*)'", r': "\1"', norm)
        norm = re.sub(r":\s*(\$[a-zA-Z_]\w*)", r': "\1"', norm)

        try:
            return json.loads(norm)
        except Exception:
            try:
                return ast.literal_eval(prop_str)
            except Exception:
                return {}

    @classmethod
    def _parse_set_ops(cls, raw_str: str) -> List[Dict[str, Any]]:
        """Parse comma-separated SET operations: var.prop = val, var:Label."""
        parts = [p.strip() for p in raw_str.split(",") if p.strip()]
        set_ops = []
        for item in parts:
            m_prop = re.match(r"^([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\s*=\s*(.*)$", item)
            if m_prop:
                var_name = m_prop.group(1)
                prop_key = m_prop.group(2)
                raw_val = m_prop.group(3).strip()
                if (raw_val.startswith("'") and raw_val.endswith("'")) or (raw_val.startswith('"') and raw_val.endswith('"')):
                    parsed_val = raw_val[1:-1]
                elif raw_val.startswith("$"):
                    parsed_val = raw_val
                elif raw_val.lower() == "true":
                    parsed_val = True
                elif raw_val.lower() == "false":
                    parsed_val = False
                elif raw_val.lower() == "null":
                    parsed_val = None
                else:
                    try:
                        parsed_val = int(raw_val)
                    except ValueError:
                        try:
                            parsed_val = float(raw_val)
                        except ValueError:
                            parsed_val = raw_val
                set_ops.append({"var": var_name, "prop": prop_key, "val": parsed_val})
            else:
                m_lbl = re.match(r"^([a-zA-Z_]\w*):([a-zA-Z_]\w*)$", item)
                if m_lbl:
                    set_ops.append({"var": m_lbl.group(1), "label": m_lbl.group(2)})
        return set_ops

    @classmethod
    def extract_template(cls, query: str, params: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """Extract or normalize literals and parameters into a canonical template string and parameter map."""
        user_params = dict(params) if params else {}
        extracted: Dict[str, Any] = {}
        param_idx = 0

        def repl_prop_map(m: re.Match) -> str:
            nonlocal param_idx
            content = m.group(1)
            if "vecf32" in content:
                return m.group(0)

            pattern = r"([a-zA-Z_]\w*\s*:\s*)('[^']*'|\"[^\"]*\"|-?\d+(?:\.\d+)?|\btrue\b|\bfalse\b|\bnull\b|\$[a-zA-Z_]\w*|\{\s*[a-zA-Z_]\w*\s*\})"
            def repl_val(vm: re.Match) -> str:
                nonlocal param_idx
                k_part = vm.group(1)
                raw_v = vm.group(2).strip()

                val: Any = None
                if raw_v.startswith("$"):
                    pname = raw_v[1:]
                    if pname in user_params:
                        val = user_params[pname]
                    else:
                        return vm.group(0)
                elif raw_v.startswith("{") and raw_v.endswith("}"):
                    pname = raw_v[1:-1].strip()
                    if pname in user_params:
                        val = user_params[pname]
                    else:
                        return vm.group(0)
                elif (raw_v.startswith("'") and raw_v.endswith("'")) or (raw_v.startswith('"') and raw_v.endswith('"')):
                    val = raw_v[1:-1]
                elif raw_v.lower() == "true":
                    val = True
                elif raw_v.lower() == "false":
                    val = False
                elif raw_v.lower() == "null":
                    val = None
                else:
                    try:
                        val = int(raw_v)
                    except ValueError:
                        try:
                            val = float(raw_v)
                        except ValueError:
                            val = raw_v

                tpl_param = f"__p{param_idx}"
                param_idx += 1
                extracted[tpl_param] = val
                return f"{k_part}${tpl_param}"

            new_content = re.sub(pattern, repl_val, content)
            return "{" + new_content + "}"

        template = re.sub(r"\{([^{}]*:[^{}]*)\}", repl_prop_map, query.strip())
        for k, v in user_params.items():
            if k not in extracted:
                extracted[k] = v
        return template, extracted

    @classmethod
    def _bind_params_to_ast(cls, obj: Any, params: Dict[str, Any]) -> Any:
        """Deeply bind parameter values into an AST details dictionary or structure."""
        if isinstance(obj, dict):
            new_dict = {}
            for k, v in obj.items():
                if k in ("where", "raw_query") and isinstance(v, str):
                    new_dict[k] = substitute_params(v, params)
                else:
                    new_dict[k] = cls._bind_params_to_ast(v, params)
            return new_dict
        elif isinstance(obj, list):
            return [cls._bind_params_to_ast(elem, params) for elem in obj]
        elif isinstance(obj, str):
            if obj.startswith("$") and obj[1:] in params:
                return params[obj[1:]]
            return obj
        else:
            return obj

    @classmethod
    def _raw_parse(cls, q: str) -> CypherStatement:
        """Parse query string directly without caching."""
        q_upper = q.upper()

        # 0. EXPLAIN / PROFILE introspection prefixes
        if q_upper.startswith("EXPLAIN"):
            inner_q = q[7:].strip()
            inner_stmt = cls.parse(inner_q)
            return CypherStatement("EXPLAIN", {"inner": inner_stmt, "raw_query": inner_q})

        if q_upper.startswith("PROFILE"):
            inner_q = q[7:].strip()
            inner_stmt = cls.parse(inner_q)
            return CypherStatement("PROFILE", {"inner": inner_stmt, "raw_query": inner_q})

        # 1. CALL procedure
        if q_upper.startswith("CALL"):
            return cls._parse_call(q)

        # 2. DROP INDEX / DROP VECTOR INDEX
        if re.match(r"^DROP\s+(VECTOR\s+)?INDEX", q, re.IGNORECASE):
            return cls._parse_drop_index(q)

        # 3. CREATE INDEX / CREATE VECTOR INDEX
        if re.match(r"^CREATE\s+(VECTOR\s+)?INDEX", q, re.IGNORECASE):
            return cls._parse_create_index(q)

        # 4. MERGE clause (Upsert)
        if q_upper.startswith("MERGE"):
            return cls._parse_merge(q)

        # 5. CREATE clause
        if q_upper.startswith("CREATE"):
            return cls._parse_create(q)

        # 6. MATCH clause
        if q_upper.startswith("MATCH"):
            return cls._parse_match(q)

        # 7. Simple RETURN expression
        if q_upper.startswith("RETURN"):
            return CypherStatement("RETURN_EXPR", {"expr": q[6:].strip()})

        raise ValueError(f"Unsupported Cypher query shape: {q}")

    @classmethod
    def parse(cls, query: str, params: Optional[Dict[str, Any]] = None) -> CypherStatement:
        """Parse a Cypher query with intelligent AST template caching and parameter substitution."""
        q_strip = query.strip()
        resolved_q = substitute_params(q_strip, params)

        # Tier 1: Check exact resolved query plan cache (O(1) hit)
        cached = _COMPILED_PLAN_CACHE.get(resolved_q)
        if cached is not None:
            return cached

        # Tier 2: Parameterized Template Cache
        template_q, extracted_params = cls.extract_template(q_strip, params)
        if template_q in _TEMPLATE_PLAN_CACHE:
            template_stmt = _TEMPLATE_PLAN_CACHE.get(template_q)
            bound_details = cls._bind_params_to_ast(template_stmt.details, extracted_params)
            stmt = CypherStatement(template_stmt.type, bound_details)
            _COMPILED_PLAN_CACHE.set(resolved_q, stmt)
            return stmt

        # Tier 3: Parse template or raw query
        if template_q != resolved_q:
            try:
                template_stmt = cls._raw_parse(template_q)
                _TEMPLATE_PLAN_CACHE.set(template_q, template_stmt)
                bound_details = cls._bind_params_to_ast(template_stmt.details, extracted_params)
                stmt = CypherStatement(template_stmt.type, bound_details)
                _COMPILED_PLAN_CACHE.set(resolved_q, stmt)
                return stmt
            except Exception:
                pass

        stmt = cls._raw_parse(resolved_q)
        _COMPILED_PLAN_CACHE.set(resolved_q, stmt)
        return stmt

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all compiled and template query caches."""
        _COMPILED_PLAN_CACHE.clear()
        _TEMPLATE_PLAN_CACHE.clear()

    @classmethod
    def get_cache_stats(cls) -> Dict[str, Any]:
        """Return cache hit/miss statistics for plan caches."""
        return {
            "compiled_plan_cache": _COMPILED_PLAN_CACHE.stats(),
            "template_plan_cache": _TEMPLATE_PLAN_CACHE.stats(),
        }

    @classmethod
    def _parse_call(cls, q: str) -> CypherStatement:
        proc_match = re.search(r"CALL\s+([\w\.]+)\s*\(", q, re.IGNORECASE)
        if not proc_match:
            # Check for CALL proc without parens
            m_noparen = re.search(r"CALL\s+([\w\.]+)", q, re.IGNORECASE)
            if m_noparen:
                proc_name = m_noparen.group(1).strip()
                remainder = q[m_noparen.end():].strip()
                yield_raw = None
                yield_match = re.search(r"^YIELD\s+([\w\s,]+)", remainder, re.IGNORECASE)
                if yield_match:
                    yield_raw = yield_match.group(1).strip()
                    remainder = remainder[yield_match.end():].strip()
                return CypherStatement("CALL", {
                    "procedure": proc_name,
                    "args": [],
                    "yield": [y.strip() for y in yield_raw.split(",")] if yield_raw else [],
                    "chained_query": remainder if remainder else None,
                })
            raise ValueError(f"Malformed CALL statement: {q}")

        proc_name = proc_match.group(1).strip()
        start_idx = proc_match.end() - 1  # Index of '('

        # Find matching ')' with paren_depth counter
        paren_depth = 0
        end_idx = -1
        for i in range(start_idx, len(q)):
            if q[i] == '(':
                paren_depth += 1
            elif q[i] == ')':
                paren_depth -= 1
                if paren_depth == 0:
                    end_idx = i
                    break

        if end_idx == -1:
            raise ValueError(f"Unbalanced parentheses in CALL statement: {q}")

        args_raw = q[start_idx + 1:end_idx].strip()
        remainder = q[end_idx + 1:].strip()

        # Check for YIELD
        yield_raw = None
        yield_match = re.search(r"^YIELD\s+([\w\s,]+)", remainder, re.IGNORECASE)
        if yield_match:
            yield_raw = yield_match.group(1).strip()
            remainder = remainder[yield_match.end():].strip()

        chained_cypher = remainder if remainder else None

        # Parse arguments
        args = []
        if args_raw:
            clean_args = re.sub(r"vecf32\s*\(\s*(\[[^\]]+\])\s*\)", r"\1", args_raw)
            try:
                args = ast.literal_eval(f"[{clean_args}]")
            except Exception:
                args = [a.strip().strip("'\"") for a in clean_args.split(",")]

        return CypherStatement("CALL", {
            "procedure": proc_name,
            "args": args,
            "yield": [y.strip() for y in yield_raw.split(",")] if yield_raw else [],
            "chained_query": chained_cypher,
        })

    @classmethod
    def _parse_create_index(cls, q: str) -> CypherStatement:
        is_vector = bool(re.search(r"VECTOR\s+INDEX", q, re.IGNORECASE))
        label_match = re.search(r"(?:FOR\s+\(\w+:|ON\s+:)(\w+)", q, re.IGNORECASE)
        prop_match = re.search(r"(?:ON\s+\(\w+\.|ON\s+:\w+\(|\bINDEX\s+ON\s+\w+\.)(\w+)", q, re.IGNORECASE)
        options_match = re.search(r"OPTIONS\s+(\{.*?\})", q, re.IGNORECASE)

        label = label_match.group(1) if label_match else None
        prop = prop_match.group(1) if prop_match else None
        options = cls.parse_properties(options_match.group(1)) if options_match else {}

        return CypherStatement("CREATE_INDEX", {
            "is_vector": is_vector,
            "label": label,
            "property": prop,
            "options": options,
        })

    @classmethod
    def _parse_drop_index(cls, q: str) -> CypherStatement:
        is_vector = bool(re.search(r"VECTOR\s+INDEX", q, re.IGNORECASE))
        label_match = re.search(r"(?:FOR\s+\(\w+:|ON\s+:)(\w+)", q, re.IGNORECASE)
        prop_match = re.search(r"(?:ON\s+\(\w+\.|ON\s+:\w+\(|\bINDEX\s+ON\s+\w+\.)(\w+)", q, re.IGNORECASE)
        if not prop_match:
            prop_match = re.search(r"ON\s+\(?\w*\.?(\w+)\)?", q, re.IGNORECASE)

        label = label_match.group(1) if label_match else None
        prop = prop_match.group(1) if prop_match else None

        return CypherStatement("DROP_INDEX", {
            "is_vector": is_vector,
            "label": label,
            "property": prop,
        })

    @classmethod
    def _parse_merge(cls, q: str) -> CypherStatement:
        on_create_match = re.search(r"\bON\s+CREATE\s+SET\b\s+(.*?)(?=\bON\s+MATCH\s+SET\b|\bRETURN\b|$)", q, re.IGNORECASE | re.DOTALL)
        on_match_match = re.search(r"\bON\s+MATCH\s+SET\b\s+(.*?)(?=\bRETURN\b|$)", q, re.IGNORECASE | re.DOTALL)
        ret_match = re.search(r"\bRETURN\b\s+(.*?)$", q, re.IGNORECASE | re.DOTALL)

        merge_end = min(
            [m.start() for m in [on_create_match, on_match_match, ret_match] if m is not None] or [len(q)]
        )
        merge_body = q[5:merge_end].strip()

        m = re.search(r"\(([a-zA-Z_]\w*)(?::([a-zA-Z_]\w*))?(?:\s*(\{.*?\}))?\)", merge_body)
        if not m:
            raise ValueError(f"Malformed MERGE clause: {merge_body}")

        var = m.group(1)
        lbl = m.group(2)
        props = cls.parse_properties(m.group(3)) if m.group(3) else {}

        on_create_set = cls._parse_set_ops(on_create_match.group(1)) if on_create_match else []
        on_match_set = cls._parse_set_ops(on_match_match.group(1)) if on_match_match else []
        ret_clause = ret_match.group(1).strip() if ret_match else None

        return CypherStatement("MERGE", {
            "var": var,
            "label": lbl,
            "properties": props,
            "on_create_set": on_create_set,
            "on_match_set": on_match_set,
            "return": ret_clause,
            "raw_query": q,
        })

    @classmethod
    def _parse_create(cls, q: str) -> CypherStatement:
        ret_match = re.search(r"\bRETURN\b\s+(.*)$", q, re.IGNORECASE | re.DOTALL)
        ret_clause = ret_match.group(1).strip() if ret_match else None
        create_body = q[6:ret_match.start() if ret_match else len(q)].strip()

        edges = []
        edge_spans = []
        edge_matches = list(re.finditer(r"\(([a-zA-Z_]\w*)\)-\[(?:([a-zA-Z_]\w*))?:([a-zA-Z_]\w*)(?:\s*(\{.*?\}))?\]->\(([a-zA-Z_]\w*)\)", create_body))
        for m in edge_matches:
            src_var = m.group(1)
            rel_var = m.group(2) or ""
            rel = m.group(3)
            props = cls.parse_properties(m.group(4)) if m.group(4) else {}
            dst_var = m.group(5)
            edges.append({"src_var": src_var, "rel_var": rel_var, "rel": rel, "dst_var": dst_var, "properties": props})
            edge_spans.append((m.start(), m.end()))

        nodes = []
        node_matches = list(re.finditer(r"\(([a-zA-Z_]\w*)(?::([a-zA-Z_]\w*))?(?:\s*(\{.*?\}))?\)", create_body))
        for m in node_matches:
            is_in_edge = any(start <= m.start() and m.end() <= end for start, end in edge_spans)
            if not is_in_edge:
                var = m.group(1)
                lbl = m.group(2)
                props = cls.parse_properties(m.group(3)) if m.group(3) else {}
                nodes.append({"var": var, "label": lbl, "properties": props})

        return CypherStatement("CREATE", {
            "nodes": nodes,
            "edges": edges,
            "return": ret_clause,
        })

    @classmethod
    def _parse_match(cls, q: str) -> CypherStatement:
        sp_match = re.search(r"shortestPath\s*\(\s*\(([a-zA-Z_]\w*)\)\s*-\[\*\]->\s*\(([a-zA-Z_]\w*)\)\s*\)", q, re.IGNORECASE)
        if sp_match:
            return CypherStatement("SHORTEST_PATH", {
                "src_var": sp_match.group(1),
                "dst_var": sp_match.group(2),
                "raw_query": q,
            })

        where_match = re.search(r"\bWHERE\b\s+(.*?)(?=\bSET\b|\bREMOVE\b|\bDELETE\b|\bDETACH\b|\bRETURN\b|\bORDER\b|\bSKIP\b|\bOFFSET\b|\bLIMIT\b|$)", q, re.IGNORECASE | re.DOTALL)
        set_match = re.search(r"\bSET\b\s+(.*?)(?=\bREMOVE\b|\bDELETE\b|\bDETACH\b|\bRETURN\b|\bORDER\b|\bSKIP\b|\bOFFSET\b|\bLIMIT\b|$)", q, re.IGNORECASE | re.DOTALL)
        remove_match = re.search(r"\bREMOVE\b\s+(.*?)(?=\bDELETE\b|\bDETACH\b|\bRETURN\b|\bORDER\b|\bSKIP\b|\bOFFSET\b|\bLIMIT\b|$)", q, re.IGNORECASE | re.DOTALL)
        del_match = re.search(r"\b(DETACH\s+)?DELETE\b\s+(.*?)(?=\bRETURN\b|\bORDER\b|\bSKIP\b|\bOFFSET\b|\bLIMIT\b|$)", q, re.IGNORECASE | re.DOTALL)
        ret_match = re.search(r"\bRETURN\b\s+(.*?)(?=\bORDER\b|\bSKIP\b|\bOFFSET\b|\bLIMIT\b|$)", q, re.IGNORECASE | re.DOTALL)
        order_match = re.search(r"\bORDER\s+BY\b\s+(.*?)(?=\bSKIP\b|\bOFFSET\b|\bLIMIT\b|$)", q, re.IGNORECASE | re.DOTALL)
        skip_match = re.search(r"\b(?:SKIP|OFFSET)\b\s+(\d+)", q, re.IGNORECASE)
        limit_match = re.search(r"\bLIMIT\b\s+(\d+)", q, re.IGNORECASE)

        match_end = min(
            [m.start() for m in [where_match, set_match, remove_match, del_match, ret_match, order_match, skip_match, limit_match] if m is not None] or [len(q)]
        )
        match_pattern = q[5:match_end].strip()

        node_parts = re.split(r"-\[(?:[a-zA-Z_]\w*)?(?::[a-zA-Z_]\w*)?(?:\s*\{.*?\})?\]->", match_pattern)
        rel_matches = list(re.finditer(r"-\[(?:([a-zA-Z_]\w*))?(?::([a-zA-Z_]\w*))?(?:\s*(\{.*?\}))?\]->", match_pattern))

        parsed_nodes = []
        for np in node_parts:
            m = re.search(r"\(([a-zA-Z_]\w*)?(?::([a-zA-Z_]\w*))?(?:\s*(\{.*?\}))?\)", np.strip())
            if m:
                var = m.group(1) or ""
                lbl = m.group(2)
                props = cls.parse_properties(m.group(3)) if m.group(3) else {}
                parsed_nodes.append({"var": var, "label": lbl, "properties": props})

        rel_info = []
        rels = []
        for rm in rel_matches:
            r_var = rm.group(1) or ""
            r_type = rm.group(2) or ""
            r_props = cls.parse_properties(rm.group(3)) if rm.group(3) else {}
            rel_info.append({"var": r_var, "rel": r_type, "properties": r_props})
            if r_type:
                rels.append(r_type)

        set_ops = cls._parse_set_ops(set_match.group(1)) if set_match else []

        remove_ops = []
        if remove_match:
            parts = [p.strip() for p in remove_match.group(1).split(",") if p.strip()]
            for item in parts:
                m_prop = re.match(r"^([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)$", item)
                if m_prop:
                    remove_ops.append({"var": m_prop.group(1), "prop": m_prop.group(2)})
                else:
                    m_lbl = re.match(r"^([a-zA-Z_]\w*):([a-zA-Z_]\w*)$", item)
                    if m_lbl:
                        remove_ops.append({"var": m_lbl.group(1), "label": m_lbl.group(2)})

        delete_targets = []
        if del_match:
            delete_targets = [t.strip() for t in del_match.group(2).split(",") if t.strip()]

        order_by = []
        if order_match:
            parts = [p.strip() for p in order_match.group(1).split(",") if p.strip()]
            for p in parts:
                m_desc = bool(re.search(r"\bDESC\b", p, re.IGNORECASE))
                expr = re.sub(r"\b(ASC|DESC)\b", "", p, flags=re.IGNORECASE).strip()
                order_by.append({"expr": expr, "desc": m_desc})

        return CypherStatement("MATCH", {
            "nodes": parsed_nodes,
            "rels": rels,
            "rel_info": rel_info,
            "where": where_match.group(1).strip() if where_match else None,
            "set": set_ops,
            "remove": remove_ops,
            "delete": delete_targets,
            "detach": bool(del_match and "DETACH" in del_match.group(1).upper()) if del_match and del_match.group(1) else False,
            "return": ret_match.group(1).strip() if ret_match else None,
            "order_by": order_by,
            "skip": int(skip_match.group(1)) if skip_match else None,
            "limit": int(limit_match.group(1)) if limit_match else None,
        })

