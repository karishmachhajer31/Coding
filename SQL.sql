WITH RecentRedemptions AS (
    SELECT
        r.retailerName,
        d.redemptionDate,
        d.redemptionCount,
        d.createDateTime,
        ROW_NUMBER() OVER (PARTITION BY d.redemptionDate ORDER BY d.createDateTime DESC) AS rn
    FROM
        `your_project.your_dataset.tblRedemptions-ByDay` d
    JOIN
        `your_project.your_dataset.tblRetailers` r
    ON
        d.retailerId = r.id
    WHERE
        r.retailerName = 'ABC Store'
        AND d.redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
)
SELECT
    redemptionDate,
    redemptionCount
FROM
    RecentRedemptions
WHERE
    rn = 1
ORDER BY
    redemptionDate;


=>Step 1: Find the Least and Most Redemption Counts
First, identify the dates with the least and most redemptions from the most recent redemption data:


WITH RecentRedemptions AS (
    SELECT
        r.retailerName,
        d.redemptionDate,
        d.redemptionCount,
        d.createDateTime,
        ROW_NUMBER() OVER (PARTITION BY d.redemptionDate ORDER BY d.createDateTime DESC) AS rn
    FROM
        `your_project.your_dataset.tblRedemptions-ByDay` d
    JOIN
        `your_project.your_dataset.tblRetailers` r
    ON
        d.retailerId = r.id
    WHERE
        r.retailerName = 'ABC Store'
        AND d.redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
),
LatestRedemptions AS (
    SELECT
        redemptionDate,
        redemptionCount,
        createDateTime
    FROM
        RecentRedemptions
    WHERE
        rn = 1
),
MinMaxRedemptions AS (
    SELECT
        MIN(redemptionCount) AS minRedemptionCount,
        MAX(redemptionCount) AS maxRedemptionCount
    FROM
        LatestRedemptions
)
SELECT
    'Least Redemptions' AS category,
    l.redemptionDate,
    l.redemptionCount,
    l.createDateTime
FROM
    LatestRedemptions l
JOIN
    MinMaxRedemptions m
ON
    l.redemptionCount = m.minRedemptionCount
UNION ALL
SELECT
    'Most Redemptions' AS category,
    l.redemptionDate,
    l.redemptionCount,
    l.createDateTime
FROM
    LatestRedemptions l
JOIN
    MinMaxRedemptions m
ON
    l.redemptionCount = m.maxRedemptionCount
ORDER BY
    category;
	

Example Output:

category	redemptionDate	redemptionCount	createDateTime
Least Redemptions	2023-11-03	2007	2023-11-06 11:00:00 UTC
Most Redemptions	2023-10-31	5162	2023-11-06 11:00:00 UTC


=>-- Step 1: Get the latest createDateTime for each redemptionDate
WITH LatestCreateTimes AS (
    SELECT
        d.redemptionDate,
        MAX(d.createDateTime) AS latestCreateDateTime
    FROM
        `your_project.your_dataset.tblRedemptions-ByDay` d
    JOIN
        `your_project.your_dataset.tblRetailers` r
    ON
        d.retailerId = r.id
    WHERE
        r.retailerName = 'ABC Store'
        AND d.redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
    GROUP BY
        d.redemptionDate
),

-- Step 2: Join to get the redemption counts based on latest createDateTime
RecentRedemptions AS (
    SELECT
        d.redemptionDate,
        d.redemptionCount,
        d.createDateTime
    FROM
        `your_project.your_dataset.tblRedemptions-ByDay` d
    JOIN
        LatestCreateTimes l
    ON
        d.redemptionDate = l.redemptionDate
        AND d.createDateTime = l.latestCreateDateTime
    JOIN
        `your_project.your_dataset.tblRetailers` r
    ON
        d.retailerId = r.id
    WHERE
        r.retailerName = 'ABC Store'
)

-- Step 3: Select the most recent redemption count for each redemptionDate
SELECT
    redemptionDate,
    redemptionCount
FROM
    RecentRedemptions
ORDER BY
    redemptionDate;

