-- Label unique-instance / unit-width processors as HighPriority in EXPLAIN PIPELINE.

SET max_threads = 4;
SET enable_analyzer = 1;

-- Final sort merge is inelastic; per-stream partial sort is not.
SELECT count() > 0
FROM (EXPLAIN PIPELINE inelastic = 1 SELECT number FROM numbers_mt(1000000) ORDER BY number)
WHERE explain LIKE '%MergingSorted%HighPriority%';

SELECT count() = 0
FROM (EXPLAIN PIPELINE inelastic = 1 SELECT number FROM numbers_mt(1000000) ORDER BY number)
WHERE explain LIKE '%PartialSorting%HighPriority%';

-- Final DISTINCT gathers to one stream.
SELECT count() > 0
FROM (EXPLAIN PIPELINE inelastic = 1 SELECT DISTINCT number % 10 FROM numbers_mt(1000000))
WHERE explain LIKE '%Distinct%HighPriority%';

-- Parallel replicas of ExpressionTransform stay elastic.
SELECT count() = 0
FROM (EXPLAIN PIPELINE inelastic = 1 SELECT number + 1 FROM numbers_mt(1000000))
WHERE explain LIKE '%ExpressionTransform%HighPriority%';

-- Default EXPLAIN PIPELINE does not print the flag.
SELECT count() = 0
FROM (EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000000) ORDER BY number)
WHERE explain LIKE '%HighPriority%';
