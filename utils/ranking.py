def bm25_score(tf, doc_len, avgdl=500, k1=1.2, b=0.75):
    """
    tf      = raw term frequency in this doc
    doc_len = number of tokens in this doc
    avgdl   = average doc length across the collection
    k1, b   = tuning parameters
    """
    num   = tf * (k1 + 1)
    denom = tf + k1 * (1 - b + b * (doc_len / avgdl))
    return num / denom