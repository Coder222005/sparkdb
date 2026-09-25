"""FulltextStore — Inverted Index and BM25 Full-Text Search for Node Text Attributes.

Provides lexical search across document chunks and entity descriptions.
"""
from __future__ import annotations

import collections
import logging
import math
import re
import threading
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class FulltextStore:
    """Thread-safe inverted index with BM25 ranking."""

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self._lock = threading.RLock()
        self.k1 = k1
        self.b = b
        self._inverted_index: Dict[str, Dict[int, int]] = collections.defaultdict(dict)
        self._doc_lengths: Dict[int, int] = {}
        self._avg_doc_len = 0.0

    def _tokenize(self, text: str) -> List[str]:
        """Simple case-insensitive word tokenizer."""
        return re.findall(r"\b\w+\b", text.lower())

    def index_node_text(self, node_id: int, text: str) -> None:
        """Tokenize and index text for a given node ID."""
        with self._lock:
            tokens = self._tokenize(text)
            self._doc_lengths[node_id] = len(tokens)

            # Term frequencies
            tf_map = collections.Counter(tokens)
            for term, freq in tf_map.items():
                self._inverted_index[term][node_id] = freq

            # Update avg doc length
            if self._doc_lengths:
                self._avg_doc_len = sum(self._doc_lengths.values()) / len(self._doc_lengths)

    def search(self, query: str, top_k: int = 10) -> List[Tuple[int, float]]:
        """Search nodes matching query keywords ranked by BM25."""
        with self._lock:
            q_terms = self._tokenize(query)
            if not q_terms or not self._doc_lengths:
                return []

            total_docs = len(self._doc_lengths)
            scores: Dict[int, float] = collections.defaultdict(float)

            for term in q_terms:
                if term not in self._inverted_index:
                    continue

                doc_freqs = self._inverted_index[term]
                df = len(doc_freqs)
                # BM25 IDF
                idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1.0)

                for doc_id, tf in doc_freqs.items():
                    doc_len = self._doc_lengths.get(doc_id, self._avg_doc_len)
                    denom = tf + self.k1 * (1.0 - self.b + self.b * (doc_len / (self._avg_doc_len or 1.0)))
                    score = idf * (tf * (self.k1 + 1.0)) / denom
                    scores[doc_id] += score

            ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            return ranked[:top_k]
