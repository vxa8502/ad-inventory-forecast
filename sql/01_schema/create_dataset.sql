CREATE SCHEMA IF NOT EXISTS `{project_id}.{dataset}`
OPTIONS (
    location = '{location}',
    description = 'Ad inventory forecasting dataset with Wikipedia pageview proxy data'
);
