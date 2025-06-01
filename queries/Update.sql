CREATE OR REPLACE PROCEDURE `GCP_PROJECT_ID.BQ_DATASET.update_bridge_table`(IN passed_ingestion_date DATE)
BEGIN
  INSERT INTO `GCP_PROJECT_ID.BQ_DATASET.Bridge_Table` (
    Deal_ID_FK,
    Product_ID_FK
  )
  WITH exploded AS (
    SELECT
      st.Deal_ID,
      TRIM(product) AS Product_Name
    FROM `GCP_PROJECT_ID.BQ_DATASET.staging_table` st,
         UNNEST(SPLIT(st.Product_Name, ',')) AS product
    WHERE st.ingestion_date = passed_ingestion_date
      AND st.Product_Name IS NOT NULL
  ),
  joined AS (
    SELECT
      fd.Deal_ID_SK_PK AS Deal_ID_FK,
      dp.Product_SK_PK AS Product_ID_FK
    FROM exploded e
    LEFT JOIN `GCP_PROJECT_ID.BQ_DATASET.Dim_Products` dp
      ON e.Product_Name = dp.Product
    LEFT JOIN `GCP_PROJECT_ID.BQ_DATASET.Fact_Deals` fd
      ON e.Deal_ID = fd.Deal_ID_NK
  ),
  filtered AS (
    SELECT
      j.Deal_ID_FK,
      j.Product_ID_FK
    FROM joined j
    LEFT JOIN `GCP_PROJECT_ID.BQ_DATASET.Bridge_Table` b
      ON j.Deal_ID_FK = b.Deal_ID_FK AND j.Product_ID_FK = b.Product_ID_FK
    WHERE j.Deal_ID_FK IS NOT NULL AND j.Product_ID_FK IS NOT NULL
      AND b.Deal_ID_FK IS NULL
  )
  SELECT DISTINCT * FROM filtered;
END;

CREATE OR REPLACE PROCEDURE `GCP_PROJECT_ID.BQ_DATASET.update_dim_date`(start_date DATE)
BEGIN
  DECLARE current_date DATE;
  DECLARE max_id INT64;

  SET current_date = start_date;

  SET max_id = (
    SELECT IFNULL(MAX(Date_SK_PK), 0)
    FROM `GCP_PROJECT_ID.BQ_DATASET.Dim_Date`
  );

  INSERT INTO `GCP_PROJECT_ID.BQ_DATASET.Dim_Date` (
    Date_SK_PK, Full_Date, Year, Quarter, Month, Day,
    Day_Name, Month_Name, Week, Is_Weekend
  )
  WITH DateRange AS (
    SELECT date
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', current_date, INTERVAL 1 DAY)) AS date
    WHERE NOT EXISTS (
      SELECT 1 FROM `GCP_PROJECT_ID.BQ_DATASET.Dim_Date` d WHERE d.Full_Date = date
    )
  ),
  Numbered AS (
    SELECT
      ROW_NUMBER() OVER (ORDER BY date) + max_id AS Date_SK_PK,
      date
    FROM DateRange
  )
  SELECT
    Date_SK_PK,
    date AS Full_Date,
    EXTRACT(YEAR FROM date) AS Year,
    EXTRACT(QUARTER FROM date) AS Quarter,
    EXTRACT(MONTH FROM date) AS Month,
    EXTRACT(DAY FROM date) AS Day,
    FORMAT_DATE('%A', date) AS Day_Name,
    FORMAT_DATE('%B', date) AS Month_Name,
    EXTRACT(WEEK FROM date) AS Week,
    EXTRACT(DAYOFWEEK FROM date) IN (1, 7) AS Is_Weekend
  FROM Numbered;
END;

CREATE OR REPLACE PROCEDURE `GCP_PROJECT_ID.BQ_DATASET.update_dim_dealstatus`(IN passed_ingestion_date DATE)
BEGIN
  MERGE `GCP_PROJECT_ID.BQ_DATASET.Dim_DealStatus` AS target
  USING (
    SELECT
      ROW_NUMBER() OVER () + (
        SELECT IFNULL(MAX(Status_ID_SK_PK), 0)
        FROM `GCP_PROJECT_ID.BQ_DATASET.Dim_DealStatus`
      ) AS Status_ID_SK_PK,
      SAFE_CAST(status_Id AS INT64) AS Status_ID_NK,
      IFNULL(Status, 'Unknown') AS Status
    FROM (
      SELECT DISTINCT status_Id, Status
      FROM `GCP_PROJECT_ID.BQ_DATASET.staging_table`
      WHERE Status IS NOT NULL AND ingestion_date = passed_ingestion_date
    )
  ) AS source
  ON target.Status_ID_NK = source.Status_ID_NK
  WHEN NOT MATCHED THEN
    INSERT (Status_ID_SK_PK, Status_ID_NK, Status)
    VALUES (source.Status_ID_SK_PK, source.Status_ID_NK, source.Status);
END;

CREATE OR REPLACE PROCEDURE `GCP_PROJECT_ID.BQ_DATASET.update_dim_organizations`(IN passed_ingestion_date DATE)
BEGIN
  MERGE `GCP_PROJECT_ID.BQ_DATASET.Dim_Organizations` AS target
  USING (
    SELECT
      ROW_NUMBER() OVER () + (
        SELECT IFNULL(MAX(Organization_ID_SK_PK), 0)
        FROM `GCP_PROJECT_ID.BQ_DATASET.Dim_Organizations`
      ) AS Organization_ID_SK_PK,
      SAFE_CAST(Organization_ID AS INT64) AS Organization_ID_NK,
      CASE
        WHEN Won_Deals IS NULL AND Won_Time IS NOT NULL THEN 1
        WHEN Won_Deals IS NULL THEN 0
        ELSE Won_Deals
      END AS Won_Deals,
      CASE
        WHEN Lost_Deals IS NULL AND Lost_Reason IS NOT NULL THEN 1
        WHEN Lost_Deals IS NULL THEN 0
        ELSE Lost_Deals
      END AS Lost_Deals,
      IFNULL(Region, 'Unknown') AS Region,
      IFNULL(CAST(Employee_Count AS STRING), 'Unknown') AS Employee_Count,
      IFNULL(Industry, 'Unknown') AS Industry
    FROM (
      SELECT DISTINCT
        Organization_ID,
        Won_Deals,
        Lost_Deals,
        Region,
        Employee_Count,
        Industry,
        Won_Time,
        Lost_Reason
      FROM `GCP_PROJECT_ID.BQ_DATASET.staging_table`
      WHERE Organization_ID IS NOT NULL
        AND ingestion_date = passed_ingestion_date
    )
  ) AS source
  ON target.Organization_ID_NK = source.Organization_ID_NK
  WHEN NOT MATCHED THEN
    INSERT (
      Organization_ID_SK_PK,
      Organization_ID_NK,
      Won_Deals,
      Lost_Deals,
      Region,
      Employee_Count,
      Industry
    )
    VALUES (
      source.Organization_ID_SK_PK,
      source.Organization_ID_NK,
      source.Won_Deals,
      source.Lost_Deals,
      source.Region,
      source.Employee_Count,
      source.Industry
    );
END;

CREATE OR REPLACE PROCEDURE `GCP_PROJECT_ID.BQ_DATASET.update_dim_owners`(IN passed_ingestion_date DATE)
BEGIN
  MERGE `GCP_PROJECT_ID.BQ_DATASET.Dim_Owners` AS target
  USING (
    SELECT DISTINCT
      SAFE_CAST(Owner_Id AS INT64) AS Owner_ID_NK,
      Owner
    FROM `GCP_PROJECT_ID.BQ_DATASET.staging_table`
    WHERE Owner IS NOT NULL
      AND ingestion_date = passed_ingestion_date
  ) AS source
  ON target.Owner_ID_NK = source.Owner_ID_NK
  WHEN NOT MATCHED THEN
    INSERT (Owner_ID_SK_PK, Owner_ID_NK, Owner)
    VALUES (
      (SELECT IFNULL(MAX(Owner_ID_SK_PK), 0) + 1 FROM `GCP_PROJECT_ID.BQ_DATASET.Dim_Owners`),
      source.Owner_ID_NK,
      source.Owner
    );
END;

CREATE OR REPLACE PROCEDURE `GCP_PROJECT_ID.BQ_DATASET.update_dim_products`(IN passed_ingestion_date DATE)
BEGIN
  DECLARE max_sk INT64;

  SET max_sk = (
    SELECT IFNULL(MAX(Product_SK_PK), 0)
    FROM `GCP_PROJECT_ID.BQ_DATASET.Dim_Products`
  );

  MERGE `GCP_PROJECT_ID.BQ_DATASET.Dim_Products` AS target
  USING (
    WITH SplitProducts AS (
      SELECT DISTINCT TRIM(product) AS Product
      FROM `GCP_PROJECT_ID.BQ_DATASET.staging_table`,
           UNNEST(SPLIT(Product_Name, ',')) AS product
      WHERE ingestion_date = passed_ingestion_date
            AND Product_Name IS NOT NULL
    ),
    Numbered AS (
      SELECT
        Product,
        ROW_NUMBER() OVER () + max_sk AS Product_SK_PK
      FROM SplitProducts
    )
    SELECT * FROM Numbered
  ) AS source
  ON target.Product = source.Product
  WHEN NOT MATCHED THEN
    INSERT (Product_SK_PK, Product)
    VALUES (source.Product_SK_PK, source.Product);
END;


CREATE OR REPLACE PROCEDURE `GCP_PROJECT_ID.BQ_DATASET.update_fact_deals`(IN passed_ingestion_date DATE)
BEGIN
  DECLARE max_deal_id_sk_pk INT64;
  DECLARE insert_count INT64;

  SET max_deal_id_sk_pk = (
    SELECT IFNULL(MAX(Deal_ID_SK_PK), 0)
    FROM `GCP_PROJECT_ID.BQ_DATASET.Fact_Deals`
  );

  INSERT INTO `GCP_PROJECT_ID.BQ_DATASET.Fact_Deals` (
    Deal_ID_SK_PK,
    Deal_ID_NK,
    Product_Amount,
    Product_Quantity,
    Deal_Value,
    Won_Time,
    Deal_Created_Date,
    Lost_Reason,
    Organization_SK,
    Owner_ID_FK,
    Status_ID_FK,
    Date_FK
  )
  SELECT
    ROW_NUMBER() OVER (ORDER BY Deal_ID) + max_deal_id_sk_pk AS Deal_ID_SK_PK,
    CAST(Deal_ID AS INT64) AS Deal_ID_NK,
    Product_Amount,
    Product_Quantity,
    Deal_Value,
    Won_Time,
    Deal_Created_Date,
    Lost_Reason,
    IFNULL(Organization_ID_SK_PK, -1),
    IFNULL(Owner_ID_SK_PK, -1),
    IFNULL(Status_ID_SK_PK, -1),
    IFNULL(Date_SK_PK, -1)
  FROM (
    SELECT
      st.Deal_ID,
      st.Product_Amount,
      st.Product_Quantity,
      st.Deal_Value,
      CAST(st.Won_Time AS DATE) AS Won_Time,
      CAST(st.Deal_Created_Date AS DATE) AS Deal_Created_Date,
      st.Lost_Reason,
      org.Organization_ID_SK_PK,
      own.Owner_ID_SK_PK,
      stat.Status_ID_SK_PK,
      date.Date_SK_PK
    FROM (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY Deal_ID ORDER BY ingestion_date DESC) AS row_num
        FROM `GCP_PROJECT_ID.BQ_DATASET.staging_table`
        WHERE ingestion_date = passed_ingestion_date
      )
      WHERE row_num = 1
    ) AS st
    LEFT JOIN `GCP_PROJECT_ID.BQ_DATASET.Dim_Organizations` org
      ON SAFE_CAST(st.Organization_ID AS INT64) = org.Organization_ID_NK
    LEFT JOIN `GCP_PROJECT_ID.BQ_DATASET.Dim_Owners` own
      ON SAFE_CAST(st.Owner_Id AS INT64) = own.Owner_ID_NK
    LEFT JOIN `GCP_PROJECT_ID.BQ_DATASET.Dim_DealStatus` stat
      ON SAFE_CAST(st.Status_Id AS INT64) = stat.Status_ID_NK
    LEFT JOIN `GCP_PROJECT_ID.BQ_DATASET.Dim_Date` date
      ON CAST(st.Deal_Created_Date AS DATE) = date.Full_Date
    WHERE NOT EXISTS (
      SELECT 1
      FROM `GCP_PROJECT_ID.BQ_DATASET.Fact_Deals` fd
      WHERE fd.Deal_ID_NK = CAST(st.Deal_ID AS INT64)
    )
  );

  SET insert_count = (
    SELECT COUNT(*)
    FROM `GCP_PROJECT_ID.BQ_DATASET.staging_table`
    WHERE ingestion_date = passed_ingestion_date
  );

  SELECT FORMAT("Update complete. Checked %d staging rows for ingestion_date %s.", insert_count, CAST(passed_ingestion_date AS STRING)) AS log;
END;

INSERT INTO `GCP_PROJECT_ID.BQ_DATASET.Fact_Deals` (
  Deal_ID_SK_PK,
  Deal_ID_NK,
  Product_Amount,
  Product_Quantity,
  Deal_Value,
  Won_Time,
  Deal_Created_Date,
  Lost_Reason,
  Organization_SK,
  Owner_ID_FK,
  Status_ID_FK,
  Date_FK,
  Predicted_Deal_Value,
  Predicted_Won_Time,
  Prediction_Model,
  Prediction_Timestamp
)
VALUES (
  -2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL
);
