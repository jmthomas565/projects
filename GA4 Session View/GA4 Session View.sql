#The below code uses GA4 data in BigQuery to create a table aggregated to unique session ID. The Source, Medium and Campaign columns are then used to create Channel Groupings. The final table can then be used to analyse key onsite behaviour metrics, as well as identify which channels customers have derived from. 


#Creating the table with one row per session

CREATE TEMP TABLE session_view AS


(

SELECT

#ga_session_id is not a session unique identifier. It needs to be concatenated with user_pseudo_id to create a unique session_id. 

    CONCAT (user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST (event_params) WHERE KEY = 'ga_session_id')) AS session_id,
    user_pseudo_id,

#event_timestamp is consolodated to UTC time (not local timezone of the property)
   
    MIN (TIMESTAMP_MICROS(event_timestamp)) event_timestamp_utc,

#This sample dataset only has sessions with 0 or 1 pageviews in. On a real GA4 dataset, this number would be > 1 for most sessions. 

    COUNT (DISTINCT CASE WHEN event_name = "page_view" AND ep.key = "page_location" 
    THEN SPLIT (ep.value.string_value, "?") [SAFE_OFFSET(0)] END) AS session_total_unique_page_views,


    MIN (CASE WHEN event_name = "page_view" AND ep.key = "page_location" AND ep.value.string_value LIKE "%Apparel%" THEN 1 ELSE 0 END) AS session_contains_apparel_page_view,
    MIN (CASE WHEN event_name = "page_view" AND ep.key = "page_location" AND ep.value.string_value LIKE "%Lifestyle%" THEN 1 ELSE 0 END) AS session_contains_lifestyle_page_view,
    MIN (CASE WHEN event_name = "page_view" AND ep.key = "page_location" AND ep.value.string_value LIKE "%Redesign%" THEN 1 ELSE 0 END) as session_contains_redesign_page_view,


   MIN (CASE WHEN (SELECT value.int_value FROM UNNEST (event_params) WHERE event_name = "session_start" AND KEY = "ga_session_number") = 1 THEN "new user"
    WHEN (SELECT value.int_value FROM UNNEST (event_params) WHERE event_name = "session_start" AND KEY = "ga_session_number") > 1 THEN "returning user"
    ELSE 'unknown' END) AS user_type,

#Note that some sessions do not contain a landing page, hence why some rows will be null

    MIN (CASE WHEN (SELECT value.int_value FROM UNNEST (event_params) WHERE event_name = 'page_view' AND KEY = 'entrances') = 1 THEN (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND KEY = 'page_location') END) AS landing_page_url,

    MIN (CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND KEY = 'entrances') = 1 THEN (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND KEY = 'page_title') END) AS landing_page_title,

    COUNT(DISTINCT CONCAT(user_pseudo_id,(SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id'))) - COUNT (DISTINCT CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'session_engaged') = '1' THEN CONCAT(user_pseudo_id,(SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id')) END) AS bounced_sessions,

#Source / Medium / Campaign are taken from the event_params, becuase these are session_scoped (as oppososed to the standard columns in the raw data which are scoped to the first user session)

    MIN ((SELECT value.string_value FROM UNNEST (event_params) WHERE KEY = 'source')) AS session_source,
    MIN ((SELECT value.string_value FROM UNNEST (event_params) WHERE KEY = 'medium')) AS session_medium,
    MIN ((SELECT value.string_value FROM UNNEST (event_params) WHERE KEY = 'campaign')) AS session_campaign,

    MIN (device.category) AS device,
    MIN (geo.city) AS city,

#session_duration below is given as total number of milliseconds (per session). This is intentionally left unnaggregated, and can then be used for average session duration aggregations when surfaced in a dashboard. 

     (TIMESTAMP_DIFF (MAX (TIMESTAMP_MICROS(event_timestamp)), MIN (TIMESTAMP_MICROS(event_timestamp)), MILLISECOND)) AS session_duration

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST (event_params) AS ep
GROUP BY session_id, 
         user_pseudo_id

ORDER BY session_id
        );
 

 
#This table creates custom channel groupings, based on session_source, session_medium and session_campaign.

SELECT 
    *,
    CASE WHEN session_source = "google" AND session_medium = "organic" THEN "Google Organic"
         WHEN session_source LIKE "%googlemerchandisestore%" THEN "Google Merchandise Store"
         WHEN session_source = "google" AND session_medium IN ("cpc", "cpm") THEN "Google Ads"
         WHEN session_source = "meta" AND session_medium IN ("cpc", "cpm") THEN "Meta Ads"
         WHEN session_source IN ("meta", "facebook") THEN "Meta - Organic"
         WHEN session_source = "instagram" AND session_medium IN ("cpc", "cpm") THEN "Instagram Ads"
         WHEN session_source = "instagram" THEN "Instagram - Organic"
         WHEN session_source = "tiktok" AND session_medium IN ("cpc", "cpm") THEN "TikTok Ads"
         WHEN session_source = "tiktok" THEN "TikTok - Organic"
         WHEN session_source = "linkedin" AND session_medium IN ("cpc", "cpm") THEN "LinkedIn Ads"
         WHEN session_source = "linkedin" THEN "LinkedIn - Organic"
         WHEN session_medium = "affiliate" THEN "Affiliates"
         WHEN session_source = "partners" THEN "Partnerships"
         WHEN session_source = "dv360" THEN "Display"
         WHEN session_source LIKE "%direct%" THEN "Direct"
         WHEN session_medium = "email" THEN "Email Marketing"
         WHEN session_source = "bing" AND session_medium = "organic" THEN "Bing - Organic"
         WHEN session_source = "baidu" AND session_medium = "organic" THEN "Baidu - Organic"
         WHEN session_source LIKE "%data deleted%" OR session_medium LIKE "%data deleted%" OR session_campaign LIKE "%data deleted%" THEN "Data Deleted"
         ELSE "Other"

END AS channel

FROM session_view









 
