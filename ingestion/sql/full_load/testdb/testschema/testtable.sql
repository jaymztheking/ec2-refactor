COPY INTO TEST_TABLE FROM (
select
    $1::integer id
    ,$2 name
    ,$3 fav_color
    ,$4::timestamp_ntz created_date
    ,$5::timestamp_ntz modified_date
from @EC2_REFACTOR_S3_STAGE);