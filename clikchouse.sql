kubectl exec -it jupyter-spark-545795577c-n4m8m -- bash

clickhouse-client --host=clickhouse-7.clickhouse.clickhouse --user=arozanov_370043 --ask-password

/*Create a table for transactions*/

CREATE TABLE arozanov_370043.transactions ON CLUSTER
kube_clickhouse_cluster
(
amount Float64,
datetime DateTime,
important Int64,
user_id_in Int64,
user_id_out Int64
)
ENGINE = MergeTree()
PARTITION BY toYear(datetime)
ORDER BY (user_id_out, user_id_in);

/*Create a distribution table based on the previos one*/

CREATE TABLE arozanov_370043.distr_transactions ON CLUSTER kube_clickhouse_cluster AS arozanov_370043.transactions
ENGINE = Distributed(kube_clickhouse_cluster, arozanov_370043, transactions, xxHash64(datetime))

/*Upload data from the existing file to the table*/

cat transactions_12M.parquet | clickhouse-client --user=arozanov_370043 --host=clickhouse-4.clickhouse.clickhouse --ask-password --query='INSERT INTO arozanov_370043.distr_transactions FORMAT PARQUET'

/*Getting the first 5 rows from the table*/

select * from arozanov_370043.distr_transactions limit 5

/* 1. Average amount for incoming and outcoming transactions by months and days for each user.*/
CREATE MATERIALIZED VIEW arozanov_370043.avg_month ON CLUSTER kube_clickhouse_cluster
ENGINE = AggregatingMergeTree
ORDER BY (user_id, date) POPULATE AS
SELECT
    table_1.user_id AS user_id,
    table_1.date,
    avg_in,
    avg_out
FROM 
(
	SELECT
            user_id_in AS user_id,
            formatDateTime(datetime, '%m-%G') AS date,
            ROUND(AVG(amount), 2) AS avg_in
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date
) AS table_1
INNER JOIN 
(
	SELECT
            user_id_out AS user_id,
            formatDateTime(datetime, '%m-%G') AS date,
            ROUND(AVG(amount), 2) AS avg_out
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date

)
AS table_2  ON (table_2.user_id, table_2.date) = (table_1.user_id, table_1.date)
ORDER BY table_1.user_id, date

CREATE TABLE arozanov_370043.distr_avg_month ON CLUSTER kube_clickhouse_cluster AS arozanov_370043.avg_month
ENGINE = Distributed(kube_clickhouse_cluster, arozanov_370043, avg_month)

CREATE MATERIALIZED VIEW arozanov_370043.avg_day ON CLUSTER kube_clickhouse_cluster
ENGINE = AggregatingMergeTree
ORDER BY (user_id, date) POPULATE AS
SELECT
    table_1.user_id AS user_id,
    table_1.date,
    avg_in,
    avg_out
FROM
(
        SELECT
            user_id_in AS user_id,
            formatDateTime(datetime, '%d-%m-%G') AS date,
            ROUND(AVG(amount), 2) AS avg_in
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date
) AS table_1
INNER JOIN
(
        SELECT
            user_id_out AS user_id,
            formatDateTime(datetime, '%d-%m-%G') AS date,
            ROUND(AVG(amount), 2) AS avg_out
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date
) AS table_2 ON (table_2.user_id, table_2.date) = (table_1.user_id, table_1.date)
ORDER BY table_1.user_id, date

CREATE TABLE arozanov_370043.distr_avg_day ON CLUSTER kube_clickhouse_cluster AS arozanov_370043.avg_day
ENGINE = Distributed(kube_clickhouse_cluster, arozanov_370043, avg_day)

/* 2. The number of important transactions for incoming and outcoming transactions by months and days for each user. */

CREATE MATERIALIZED VIEW arozanov_370043.important_day ON CLUSTER kube_clickhouse_cluster
ENGINE = AggregatingMergeTree
ORDER BY (user_id, date) POPULATE AS
SELECT
    table_1.user_id AS user_id,
    table_1.date AS date,
    cnt_in,
    cnt_out
FROM 
(
        SELECT
            user_id_in AS user_id,
            formatDateTime(datetime, '%d-%m-%G') AS date,
            COUNT(amount) AS cnt_in
        FROM arozanov_370043.distr_transactions
        WHERE important = 1
        GROUP BY
            user_id,
            date
) AS table_1
INNER JOIN
(
        SELECT
            user_id_out AS user_id,
            formatDateTime(datetime, '%d-%m-%G') AS date,
            COUNT(amount) AS cnt_out
        FROM arozanov_370043.distr_transactions
        WHERE important = 1
        GROUP BY
            user_id,
            date
) AS table_2 ON (table_2.user_id = table_1.user_id) AND (table_2.date = table_1.date)
ORDER BY date

CREATE TABLE arozanov_370043.distr_important_day ON CLUSTER kube_clickhouse_cluster AS arozanov_370043.important_day
ENGINE = Distributed(kube_clickhouse_cluster, arozanov_370043, important_day)

CREATE MATERIALIZED VIEW arozanov_370043.important_month ON CLUSTER kube_clickhouse_cluster
ENGINE = AggregatingMergeTree
ORDER BY (user_id, date) POPULATE AS
SELECT
    table_1.user_id AS user_id,
    table_1.date AS date,
    cnt_in,
    cnt_out
FROM
(
        SELECT
            user_id_in AS user_id,
            formatDateTime(datetime, '%m-%G') AS date,
            COUNT(amount) AS cnt_in
        FROM arozanov_370043.distr_transactions
        WHERE important = 1
        GROUP BY
            user_id,
            date
) AS table_1
INNER JOIN
(
        SELECT
            user_id_out AS user_id,
            formatDateTime(datetime, '%m-%G') AS date,
            COUNT(amount) AS cnt_out
        FROM arozanov_370043.distr_transactions
        WHERE important = 1
        GROUP BY
            user_id,
            date
) AS table_2 ON (table_2.user_id = table_1.user_id) AND (table_2.date = table_1.date)
ORDER BY date

CREATE TABLE arozanov_370043.distr_important_month ON CLUSTER kube_clickhouse_cluster AS arozanov_370043.important_month
ENGINE = Distributed(kube_clickhouse_cluster, arozanov_370043, important_month)

/* 3. The sums for incoming and outcoming transactions by months for each user. */

CREATE MATERIALIZED VIEW arozanov_370043.sum_month ON CLUSTER kube_clickhouse_cluster
ENGINE = AggregatingMergeTree
ORDER BY (user_id, date) POPULATE AS
SELECT
    table_1.user_id AS user_id,
    table_1.date,
    sum_in,
    sum_out
FROM
(
        SELECT
            user_id_in AS user_id,
            formatDateTime(datetime, '%m-%G') AS date,
            ROUND(SUM(amount), 2) AS sum_in
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date
) AS table_1
INNER JOIN
(
        SELECT
            user_id_out AS user_id,
            formatDateTime(datetime, '%m-%G') AS date,
            ROUND(SUM(amount), 2) AS sum_out
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date
) as table_2 ON (table_2.user_id, table_2.date) = (table_1.user_id, table_1.date)
ORDER BY table_1.user_id, date

CREATE TABLE arozanov_370043.distr_sum_month ON CLUSTER kube_clickhouse_cluster AS arozanov_370043.sum_month
ENGINE = Distributed(kube_clickhouse_cluster, arozanov_370043, sum_month)

/* 4. Users’ saldo for the current moment. */

CREATE MATERIALIZED VIEW arozanov_370043.saldo ON CLUSTER kube_clickhouse_cluster
ENGINE = AggregatingMergeTree
ORDER BY (user_id, date) POPULATE AS
SELECT
    table_1.user_id AS user_id,
    table_1.date,
    sum_in,
    sum_out,
    ROUND((sum_in - sum_out),2) as saldo
FROM
(
        SELECT
            user_id_in AS user_id,
            datetime AS date,
            ROUND(SUM(amount), 2) AS sum_in
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date
) AS table_1
INNER JOIN
(
        SELECT
            user_id_out AS user_id,
            datetime AS date,
            ROUND(SUM(amount), 2) AS sum_out
        FROM arozanov_370043.distr_transactions
        GROUP BY
            user_id,
            date
) AS table_2 ON (table_2.user_id, table_2.date) = (table_1.user_id, table_1.date)
ORDER BY table_1.user_id, date

CREATE TABLE arozanov_370043.distr_saldo ON CLUSTER kube_clickhouse_cluster AS arozanov_370043.saldo
ENGINE = Distributed(kube_clickhouse_cluster, arozanov_370043, saldo)
