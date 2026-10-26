-- Question: Which data defects were rejected, in which source batches, and how often?
SELECT
    q.entity,
    reason,
    COUNT(*) AS records,
    COUNT(DISTINCT q.batch_id) AS batches,
    MIN(q.batch_id) AS first_batch,
    MAX(q.batch_id) AS last_batch
FROM silver.quarantine AS q
LATERAL VIEW EXPLODE(q.reasons) r AS reason
GROUP BY q.entity, reason
ORDER BY records DESC, q.entity, reason
