class DataCleaningQuery():
    """A class for holding queries used to clean the Sparkify 
    event log.
    """
    
    COUNT_INVALID_USER_IDS = """
        SELECT COUNT(DISTINCT userId) AS invalid_user_count
        FROM {table}
        WHERE CAST(userId AS INT) IS NULL
    """
    
    DROP_INVALID_USERS = """
        SELECT *
        FROM {table}
        WHERE CAST(userId AS INT) IS NOT NULL
    """
    
    COUNT_USERS_WITH_INVALID_REG_DATE = """
        SELECT COUNT(userId)
        FROM 
            (
            SELECT 
                userId, 
                FIRST_VALUE(registration) AS registration, 
                MAX(ts) AS min_user_ts
            FROM {table}
            GROUP BY userId
            HAVING FIRST_VALUE(registration) > MIN(ts)
            )
    """
    
    DROP_USERS_WITH_INVALID_REGISTRATION = """
        SELECT *
        FROM {table}
        WHERE userId NOT IN (
            SELECT 
                userId
            FROM 
                (
                SELECT 
                    userId, 
                    FIRST_VALUE(registration) AS registration, 
                    MAX(ts) AS min_user_ts
                FROM {table}
                GROUP BY userId
                HAVING FIRST_VALUE(registration) > MIN(ts)
            )
        )
    """
    
    COUNT_USERS_WITH_NULL_REG_DATE = """
        SELECT COUNT(DISTINCT userId)
        FROM {table}
        WHERE registration IS NULL
    """
    
    DROP_USERS_WITH_NULL_REGISTRATION = """
        SELECT *
        FROM {table}
        WHERE registration IS NOT NULL
    """
    
    
    