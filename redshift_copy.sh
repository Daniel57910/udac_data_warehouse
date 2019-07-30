copy song_staging FROM 's3://sparkify-staging-dmiller/data/song_staging.csv' iam_role 'arn:aws:iam::774141665752:role/redshift_s3_role' region 'eu-west-2' delimiter ',';

select err_code, err_reason, colname, raw_line from stl_load_errors;