MERGE INTO TEST_TABLE tgt USING(
    SELECT DISTINCT
        $1 new_id
        ,$2 new_name
        ,$3 new_fav_color
        ,$4 new_created_date
        ,$5 new_modified_date
    from @EC2_REFACTOR_S3_STAGE) src
ON
    tgt.id = src.new_id
WHEN MATCHED THEN
UPDATE SET
    id = src.new_id
    ,name = src.new_name
    ,fav_color = src.new_fav_color
    ,created_date = src.new_created_date
    ,modified_date = src.new_modified_date
WHEN NOT MATCHED THEN INSERT(
    id
    ,name
    ,fav_color
    ,created_date
    ,modified_date
) VALUES (
    src.new_id
    ,src.new_name
    ,src.new_fav_color
    ,src.new_created_date
    ,src.new_modified_date
)