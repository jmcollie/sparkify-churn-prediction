class DataPipelineQuery:
    """A class for holding queries to transform
    and combine the Sparkify dataset.
    """
    
    GET_USER_OBSERVATION_PERIOD = """
        SELECT 
            MIN(dt) AS min_dt,
            MAX(dt) AS max_dt
        FROM {table}
    """
    
    GET_USER_PAGES = """
        SELECT 
            userId,
            registration,
            gender,
            SUM(IF(page = 'NextSong', 1, 0)) AS next_songs,
            SUM(IF(page = 'Roll Advert', 1, 0)) AS rolled_adverts,
            SUM(IF(page = 'Save Settings', 1, 0)) AS save_settings,
            SUM(IF(page = 'Home', 1, 0)) AS home,
            SUM(IF(page = 'Settings', 1, 0)) AS settings,
            SUM(IF(page = 'About', 1, 0)) AS about,
            SUM(IF(page = 'Logout', 1, 0)) AS logout,
            SUM(IF(page = 'Error', 1, 0)) AS errors,
            SUM(IF(page = 'Help', 1, 0)) AS help,
            SUM(IF(page = 'Submit Downgrade', 1, 0)) AS submitted_downgrades,
            SUM(IF(page = 'Submit Upgrade', 1, 0)) AS submitted_upgrades,
            SUM(IF(page = 'Downgrade', 1, 0)) AS downgrades,
            SUM(IF(page = 'Upgrade', 1, 0)) AS upgrades,
            SUM(IF(page = 'Thumbs Up', 1, 0)) AS thumbs_up,
            SUM(IF(page = 'Thumbs Down', 1, 0)) AS thumbs_down,
            SUM(IF(page = 'Add to Playlist', 1, 0)) AS add_to_playlist,
            SUM(IF(page = 'Add Friend', 1, 0)) AS added_friends,
            SUM(IF(page = 'Cancellation Confirmation', 1, 0)) AS cancel,
            COUNT(DISTINCT sessionId) AS unique_sessions
        FROM {table}
        GROUP BY
            userId,
            registration, 
            gender
    """
    
    GET_USER_SESSIONS = """
        SELECT 
            userId, 
            MEAN(hours) AS mean_session_hours, 
            MEDIAN(hours) AS median_session_hours,
            SUM(hours) AS total_session_hours,
            SUM(session_next_song_length) AS total_next_song_length
        FROM 
            (
            SELECT 
                userId, 
                sessionId, 
                DATEDIFF(minute, MIN(dt), MAX(dt)) / 60 AS hours,
                SUM(length) AS session_next_song_length
            FROM {table}
            GROUP BY userId, sessionId
            )
        GROUP BY userId
    """
    
    GET_USER_TIMELINES = """
        SELECT
            userId,
            DATEDIFF(
                hour,
                CASE WHEN registration > :observation_start 
                THEN registration ELSE :observation_start END, 
                CASE WHEN status = 1 THEN max_user_dt 
                ELSE :observation_end END
            ) / 24 AS days_observed,
            SIZE(
                SEQUENCE(
                    TO_DATE(
                        CASE WHEN registration > :observation_start 
                        THEN registration ELSE :observation_start END
                    ), 
                    TO_DATE(
                        CASE WHEN status = 1 THEN max_user_dt
                        ELSE :observation_end END
                    ), 
                    interval 1 day
                )
            ) AS unique_days_observed,
            DATEDIFF(
                hour,
                registration,
                CASE WHEN status = 1 THEN max_user_dt
                ELSE :observation_end END
            ) / 24 AS days_registered,
            min_user_dt,
            max_user_dt,
            CASE WHEN registration > :observation_start
            THEN registration ELSE :observation_start END AS observation_start,
            CASE WHEN status = 1 THEN max_user_dt 
            ELSE :observation_end END AS observation_end
        FROM 
            (
            SELECT 
                userId, 
                registration,
                MAX(dt) AS max_user_dt,
                MIN(dt) AS min_user_dt,
                SUM(CASE WHEN page = 'Cancellation Confirmation' THEN 1 ELSE 0 END) AS status,
                COUNT(DISTINCT sessionId) AS unique_sessions
            FROM {table}
            GROUP BY userId, registration
            )
    """
    
    GET_USER_LAST_LEVEL = """
        SELECT DISTINCT
            el.userId,
            el.level 
        FROM {table} el
        JOIN 
            (
            SELECT userId, MAX(dt) AS max_user_dt
            FROM {table}
            GROUP BY userId
            ) last_event
        ON last_event.userId = el.userId
        AND last_event.max_user_dt = el.dt
    """
    
    GET_USER_DAYS_ACTIVE_BY_LEVEL = """
        SELECT 
            userId,
            COUNT(DISTINCT(TO_DATE(dt))) AS days_active,
            COUNT(
                DISTINCT CASE WHEN level = 'paid' 
                THEN TO_DATE(dt) ELSE NULL END
            ) AS paid_days_active,
            COUNT(
                DISTINCT CASE WHEN level = 'free' 
                THEN TO_DATE(dt) 
                ELSE NULL END
            ) AS free_days_active,
            SUM(length) AS length
        FROM {table}
        GROUP BY 
            userId
    """
    
    PARSE_USER_AGENTS = """
        SELECT 
            userId,
            CONTAINS(userAgent, 'Mobi') is_mobile,
            CASE 
            WHEN userAgent RLIKE 'Firefox/' AND NOT RLIKE(userAgent, 'Seamonkey') THEN 'Firefox'
            WHEN userAgent RLIKE 'Seamonkey/' THEN 'Seamonkey'
            WHEN userAgent RLIKE 'Chrome/' AND userAgent NOT RLIKE('Chromium/|Edge/') THEN 'Chrome'
            WHEN userAgent RLIKE 'Chromium/' THEN 'Chromium'
            WHEN userAgent RLIKE 'Safari/' AND userAgent NOT RLIKE('Chrome/|Chromium/') THEN 'Safari'
            WHEN userAgent RLIKE 'OPR/' THEN 'Opera'
            WHEN userAgent RLIKE 'Opera/' THEN 'Opera'
            WHEN userAgent RLIKE 'Trident/' THEN 'Internet Explorer'
            ELSE 'Other' 
            END AS browser,
            CASE
            WHEN CONTAINS(userAgent, 'Mac OS') THEN 'Mac'
            WHEN CONTAINS(userAgent, 'Windows NT') THEN 'Windows'
            ELSE 'Other' 
            END AS os
        FROM 
            (
            SELECT DISTINCT 
                userId, 
                userAgent
            FROM {table}
            )
    """
    
    PARSE_USER_LOCATIONS = """
        SELECT 
            userId,
            state
        FROM 
        (
        SELECT DISTINCT
            SPLIT(location, '[,]')[0] AS city, 
            SPLIT(REPLACE(SPLIT(location, '[,]')[1], ' '), '[-]') AS state,
            userId
        FROM {table}
        )
    """
    
    GET_USER_LAST_7_DAYS = """
        SELECT
            el.userId,
            SUM(IF(page = 'NextSong', 1, 0)) AS next_songs_last_7d,
            SUM(IF(page = 'Roll Advert', 1, 0)) AS rolled_adverts_last_7d,
            SUM(IF(page = 'Save Settings', 1, 0)) AS save_settings_last_7d,
            SUM(IF(page = 'Home', 1, 0)) AS home_last_7d,
            SUM(IF(page = 'Settings', 1, 0)) AS settings_last_7d,
            SUM(IF(page = 'About', 1, 0)) AS about_last_7d,
            SUM(IF(page = 'Logout', 1, 0)) AS logouts_last_7d,
            SUM(IF(page = 'Error', 1, 0)) AS errors_last_7d,
            SUM(IF(page = 'Help', 1, 0)) AS help_last_7d,
            SUM(IF(page = 'Submit Downgrade', 1, 0)) AS submitted_downgrades_last_7d,
            SUM(IF(page = 'Submit Upgrade', 1, 0)) AS submitted_upgrades_last_7d,
            SUM(IF(page = 'Downgrade', 1, 0)) AS downgrades_last_7d,
            SUM(IF(page = 'Upgrade', 1, 0)) AS upgrades_last_7d,
            SUM(IF(page = 'Thumbs Up', 1, 0)) AS thumbs_up_last_7d,
            SUM(IF(page = 'Thumbs Down', 1, 0)) AS thumbs_down_last_7d,
            SUM(IF(page = 'Add to Playlist', 1, 0)) AS add_to_playlist_last_7d,
            SUM(IF(page = 'Add Friend', 1, 0)) AS added_friends_last_7d,
            COUNT(DISTINCT sessionId) AS unique_sessions_last_7d
        FROM {table1} el
        JOIN
            (
            SELECT
                userId,
                TO_DATE(observation_end) AS max_user_date,
                DATEADD(observation_end, -7) AS max_user_date_minus_7d
            FROM {table2}
            ) ut
        ON el.userId = ut.userId
        AND TO_DATE(el.dt) BETWEEN ut.max_user_date_minus_7d AND ut.max_user_date
        GROUP BY el.userId
    """
    
    
    GET_USER_EVENTS = """
        SELECT 
            up.userId, 
            up.registration,
            up.gender,
            ua.browser,
            ua.os,
            CASE WHEN ua.is_mobile = 1 THEN 'Mobile' ELSE 'Non-Mobile' END AS device_type,
            ull.level AS last_level,
            MONTH(up.registration) AS cohort,
            up.next_songs/ut.days_observed AS next_songs_per_day,
            up.rolled_adverts/ut.days_observed AS rolled_adverts_per_day,
            up.submitted_downgrades/ut.days_observed AS submitted_downgrades_per_day,
            up.submitted_upgrades/ut.days_observed AS submitted_upgrades_per_day,
            up.help/ut.days_observed AS help_per_day,
            up.errors/ut.days_observed AS errors_per_day,
            up.thumbs_up/ut.days_observed AS thumbs_up_per_day,
            up.thumbs_down/ut.days_observed AS thumbs_down_per_day,
            up.add_to_playlist/ut.days_observed AS add_to_playlist_per_day,
            up.about/ut.days_observed AS about_per_day,
            up.settings/ut.days_observed AS settings_per_day,
            up.save_settings/ut.days_observed AS save_settings_per_day,
            up.upgrades/ut.days_observed AS upgrades_per_day,
            up.downgrades/ut.days_observed AS downgrades_per_day,
            up.added_friends/ut.days_observed AS added_friends_per_day,
            up.unique_sessions/ut.days_observed AS sessions_per_day,
            us.total_session_hours/ut.days_observed AS session_hours_per_day,
            up.home/ut.days_observed AS home_per_day,
            up.logout/ut.days_observed AS logouts_per_day,
            CASE WHEN IFNULL(cancel, 0) = 1 THEN 'Cancelled' ELSE 'Active' END AS status,
            cancel AS churn,
            ul.free_days_active/days_active AS free_days_active_per_days_active, 
            ul.paid_days_active/days_active AS paid_days_active_per_days_active,
            ul.length,
            :observation_start AS observation_start,
            :observation_end AS observation_end,
            us.mean_session_hours,
            us.total_session_hours,
            us.total_next_song_length,
            ut.unique_days_observed,
            ul.days_active,
            ut.days_observed,
            ut.days_registered,
            days_active/ut.unique_days_observed AS proportion_of_days_active,
            COALESCE(uld.next_songs_last_7d, 0) /IF(ut.days_observed >= 7, 7, ut.days_observed) AS next_songs_per_day_last_7d,
            COALESCE(uld.rolled_adverts_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS rolled_adverts_per_day_last_7d,
            COALESCE(uld.save_settings_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS save_settings_per_day_last_7d,
            COALESCE(uld.home_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS home_per_day_last_7d,
            COALESCE(uld.settings_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS settings_per_day_last_7d,
            COALESCE(uld.about_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS about_per_day_last_7d,
            COALESCE(uld.logouts_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS logouts_per_day_last_7d,
            COALESCE(uld.errors_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS errors_per_day_last_7d,
            COALESCE(uld.help_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS help_per_day_last_7d,
            COALESCE(uld.submitted_downgrades_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS submitted_downgrades_per_day_last_7d,
            COALESCE(uld.submitted_upgrades_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS submitted_upgrades_per_day_last_7d,
            COALESCE(uld.downgrades_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS downgrades_per_day_last_7d,
            COALESCE(uld.upgrades_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS upgrades_per_day_last_7d,
            COALESCE(uld.thumbs_up_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS thumbs_up_per_day_last_7d,
            COALESCE(uld.thumbs_down_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS thumbs_down_per_day_last_7d,
            COALESCE(uld.add_to_playlist_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS add_to_playlist_per_day_last_7d,
            COALESCE(uld.added_friends_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS added_friends_per_day_last_7d,
            COALESCE(uld.unique_sessions_last_7d, 0)/IF(ut.days_observed >= 7, 7, ut.days_observed) AS sessions_per_day_last_7d,
            up.rolled_adverts/up.next_songs AS rolled_adverts_per_next_song,
            COALESCE(
                COALESCE(uld.rolled_adverts_last_7d, 0)/COALESCE(uld.next_songs_last_7d, 0), 0
            ) AS rolled_adverts_per_next_song_last_7d,
            up.thumbs_up/up.next_songs AS thumbs_up_per_next_song,
            COALESCE(
                COALESCE(uld.thumbs_up_last_7d, 0)/COALESCE(uld.next_songs_last_7d, 0), 0
            ) AS thumbs_up_per_next_song_last_7d,
            up.thumbs_down/up.next_songs AS thumbs_down_per_next_song,
            COALESCE(
                COALESCE(uld.thumbs_down_last_7d, 0)/COALESCE(uld.next_songs_last_7d, 0), 0
            ) AS thumbs_down_per_next_song_last_7d,
            up.add_to_playlist/up.next_songs AS add_to_playlist_per_next_song,
            COALESCE(
                COALESCE(uld.add_to_playlist_last_7d, 0)/COALESCE(uld.next_songs_last_7d, 0), 0
            ) AS add_to_playlist_per_next_song_last_7d
        FROM {user_days_active_by_level} ul
        JOIN {user_pages} up
        ON ul.userId = up.userId
        JOIN {user_timelines} ut
        ON ut.userId = ul.userId
        JOIN {user_sessions} us
        ON us.userId = ul.userId
        JOIN {parsed_user_agents} ua
        ON ua.userId = ul.userId
        JOIN {parsed_user_locations} ulo
        ON ul.userId = ulo.userId
        JOIN {user_last_level} ull
        ON ull.userId = ul.userId
        LEFT JOIN {user_last_7_days} uld
        ON ul.userId = uld.userId
    """