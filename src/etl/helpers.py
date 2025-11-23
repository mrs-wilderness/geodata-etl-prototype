from sqlalchemy import insert
import pandas as pd


def insert_with_returning(conn, table, rows, returning_cols):
    """
    Execute an INSERT ... RETURNING for a batch of rows.

    Parameters:
        conn           SQLAlchemy connection inside engine.begin()
        table          SQLAlchemy Table object
        rows           list of dicts
        returning_cols list of column objects to return

    Returns:
        result         Result object (fetchall() available)
    """
    stmt = insert(table).returning(*returning_cols)
    result = conn.execute(stmt, rows)
    return result


def insert_simple(conn, table, rows):
    """
    Execute a simple INSERT (no RETURNING) for a batch of rows.

    Parameters:
        conn            SQLAlchemy connection inside engine.begin()
        table           SQLAlchemy Table object
        rows            list of dicts
    """
    stmt = insert(table)
    conn.execute(stmt, rows)


def merge_generated_ids(df, result, left_on, returned_cols, drop_cols):
    """
    Merge database-generated IDs into a DataFrame after an INSERT ... RETURNING.

    Parameters:
        df            DataFrame to merge into
        result        SQLAlchemy Result (supports fetchall)
        left_on       column in df to match on
        returned_cols list of column names returned by the insert
        drop_cols     list of columns to drop from df after merge

    Returns:
        DataFrame with new IDs merged
    """
    # convert SQL RETURNING output to DataFrame
    returned_df = pd.DataFrame(result.fetchall(), columns=returned_cols)

    # merge into original
    df = df.merge(returned_df, left_on=left_on, right_on=returned_cols[1], how='left')

    # drop the extra right-side key(s) or temp columns
    df = df.drop(columns=drop_cols)

    return df


def explode_and_strip(df, cols, sep=';'):
    df = df.copy()
    for col in cols:
        df[col] = df[col].str.split(sep)
    df = df.explode(cols)
    for col in cols:
        df[col] = df[col].str.strip()
    return df

