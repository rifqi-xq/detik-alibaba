CREATE TEMP FUNCTION
  myFuncWeightCalculation(hitOriginal INT64,
    processMillis INT64,
    createdDateMillis INT64,
    currentWeight FLOAT64,
    yesterdayWeight FLOAT64)
  RETURNS FLOAT64
  LANGUAGE js OPTIONS (library=["{{pagerank_script_path}}/pagerank.js"]) AS """
    // 'weightCalculation' is defined in one of the library files.
    if ( currentWeight == null || yesterdayWeight == null) {
        return weightCalculation(hitOriginal, processMillis,createdDateMillis,0.0,0.0 );
    } else {
        return weightCalculation(hitOriginal, processMillis,createdDateMillis,currentWeight,yesterdayWeight );
    }
""";
CREATE OR REPLACE TABLE
  `detik_mp.mospop_history_all_20241031T1000` (
    accountType STRING NOT NULL,
    kanalid STRING NOT NULL,
    parentid STRING,
    id STRING NOT NULL,
    mp_flag INT64 NOT NULL,
    domain STRING NOT NULL,
    subAccountType STRING NOT NULL,
    datetime TIMESTAMP NOT NULL,
    hitOriginal INT64 NOT NULL,
    siteId STRING,
    idType STRING,
    keyword ARRAY<STRING>,
    dateProcessed TIMESTAMP NOT NULL,
    hitWeighted FLOAT64 NOT NULL,
    url STRING NOT NULL,
    title STRING NOT NULL ) AS
SELECT
  accountType,
  kanalid,
  parentid,
  id,
  COALESCE(mp_flag,1) mp_flag,
  domain,
  subAccountType,
  datetime,
  hitOriginal,
  siteId,
  idType,
  SPLIT(keyword,",") keyword,
  TIMESTAMP "2024-10-31 10:15:00 Asia/Jakarta" AS dateProcessed,
  myFuncWeightCalculation(hitOriginal,
    UNIX_MILLIS(TIMESTAMP "2024-10-31 10:00:00 Asia/Jakarta"),
    UNIX_MILLIS(datetime),
    weightToday,
    weightYesterday) AS hitWeighted,
  "-" AS url,
  "-" AS title
FROM (
  SELECT
    accountType,
    kanalid,
    flat.parentChannel AS parentid,
    id,
    mp_flag,
    domain,
    subAccountType,
    idType,
    keyword,
    datetime,
    hitOriginal,
    (
    SELECT
      siteId
    FROM
      UNNEST(DATA)
    ORDER BY
      hitOriginal ASC
    LIMIT
      1) AS siteId,
    flat.weightToday AS weightToday,
    flat.weightYesterday AS weightYesterday
  FROM (
    SELECT
      accountType,
      kanalid,
      id,
      domain,
      subAccountType,
      MIN(mp_flag) mp_flag,
      MAX(idType) idType,
      MAX(keyword) keyword,
      MIN(datetime) AS datetime,
      SUM(hitOriginal) AS hitOriginal,
      ARRAY_AGG(STRUCT(siteId,
          hitOriginal)) AS DATA
    FROM (
      SELECT
        accountType,
        kanalid,
        id,
        siteId,
        idType,
        MIN(mp_flag) mp_flag,
        MAX(keyword) keyword,
        domain,
        subAccountType,
        MIN(datetime) AS datetime,
        SUM(hitOriginal) AS hitOriginal
      FROM (
        SELECT
          accountType,
          kanalid,
          id,
          mp_flag,
          siteId,
          idType,
          domain,
          subAccountType,
          ARRAY_TO_STRING(keyword,",") keyword,
          datetime,
          hitOriginal
        FROM (
          SELECT
            accountType,
            kanalid,
            id,
            mp_flag,
            siteId,
            idType,
            domain,
            subAccountType,
            datetime,
            hitOriginal,
            keyword
          FROM
            detik_mp.mospop_history_all_20241031T0945
          WHERE
            datetime > TIMESTAMP (CONCAT( CAST(DATE_SUB(EXTRACT(DATE
                    FROM
                      TIMESTAMP "2024-10-31 10:00:00 Asia/Jakarta"), INTERVAL 3 MONTH) AS string), CAST(" " AS string), CAST(EXTRACT(TIME
                  FROM
                    TIMESTAMP "2024-10-31 10:00:00 Asia/Jakarta") AS string), CAST(" " AS string),CAST("UTC" AS string) ) )
            AND datetime != TIMESTAMP "1970-01-01 07:00:00 UTC"
            AND mp_flag = 1)
        UNION ALL (
          SELECT
            dtmac accountType,
            kanalId kanalid,
            articleId id,
            mp_flag,
            siteId,
            keywords.idType,
            "all" domain,
            "all" subAccountType,
            MAX(IFNULL(keywords.keyword, articles.keyword)) keyword,
            MIN(
            IF
              (publishDate < TIMESTAMP_MILLIS(899917200000), TIMESTAMP_MILLIS(UNIX_MILLIS(publishDate) * 1000), publishDate)) datetime,
            MAX(cnt) hitOriginal
          FROM (
            SELECT
              dtmac,
              kanalId,
              articleId,
              siteId,
              customPageNumber,
              keywords AS keyword,
              MIN(publishDate) AS publishDate,
              COUNT(articleId) AS cnt
            FROM
              `detik_articles.20241031_1000`
            WHERE
              dtmacSub != "apps"
              AND articleId != "-"
              AND articleId != ""
              AND kanalid != "-"
              AND publishDate > TIMESTAMP "1970-01-01 07:00:00 UTC"
            GROUP BY
              dtmac,
              kanalId,
              articleId,
              siteId,
              customPageNumber,
              keyword ) articles
          LEFT JOIN (
            SELECT
              id,
              mp_flag,
              title,
              idType,
              STRING_AGG(keywords.keyword) keyword
            FROM (
              SELECT
                id,
                MIN(COALESCE(mp_flag,1)) OVER (PARTITION BY id) mp_flag,
                title,
                id_article_type idType,
                ROW_NUMBER() OVER(PARTITION BY id ORDER BY loggedtime DESC) AS rank_num,
                keywords
              FROM
                `detikcom-179007.documents.detik_documents`
              WHERE
                publishdate >= '2023-01-01' ),
              UNNEST(keywords) keywords
            WHERE
              rank_num = 1
              AND title != "deleted"
              AND mp_flag = 1
            GROUP BY
              1,
              2,
              3,
              4 ) keywords
          ON
            articles.articleid = keywords.id
          GROUP BY
            dtmac,
            kanalId,
            articleId,
            mp_flag,
            siteId,
            idType ) )
      GROUP BY
        accountType,
        subAccountType,
        domain,
        kanalid,
        id,
        siteId,
        idType )
    GROUP BY
      accountType,
      subAccountType,
      domain,
      kanalid,
      id ) AS curdata
  INNER JOIN
    `flattener.detik*` AS flat
  ON
    curdata.kanalid = flat.channel
    AND flat.dtmac = curdata.accountType )
