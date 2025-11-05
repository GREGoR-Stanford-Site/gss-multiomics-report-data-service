import argparse
import json
import time
import ipdb
import pandas as pd
import polars as pl
import numpy as np
import mygene
import requests
from functools import wraps
import hashlib

from app.config import settings

OP_MAPPINGS = {
    "start": ">=",
    "begin": ">=",
    "from": ">=",
    "end": "<=",
    "to": "<=",
    ">=": ">=",
    ">": ">",
    "<=": "<=",
    "<": "<",
    "min": ">=",
    "max": "<=",
    "lo": ">=",
    "up": "<=",
}

# for getting the gene symbols from the ensembl ids
MG_CLIENT = None


def add_hash_to_chunk(
    chunk_df, hash_columns, hash_column_name="variant_hash", hash_type="md5"
):
    """Process a chunk and add hash column"""
    hash_func = hashlib.md5 if hash_type == "md5" else hashlib.sha256

    return chunk_df.with_columns(
        [
            pl.concat_str(
                [pl.col(col).cast(pl.Utf8) for col in hash_columns], separator="|"
            )
            .map_elements(
                lambda x: hash_func(x.encode()).hexdigest(), return_dtype=pl.Utf8
            )
            .alias(hash_column_name)
        ]
    )


"""
    Utilties functions to be used by other modules
"""


def normalize_gene(gene):
    return gene


def file_chunker_pandas(
    file_obj,
    dtype=None,
    chunksize=100,
    sep="\t",
    lower_headers=False,
    skiprows=0,
    replacements=None,
    numeric_cols=None,
    **kwargs,
):
    # use Pandas to create a chunker

    for chunk in pd.read_csv(
        file_obj, dtype=dtype, skiprows=skiprows, chunksize=chunksize, delimiter=sep
    ):
        # lower case the headers
        if lower_headers:
            chunk.columns = [col.lower() for col in chunk.columns]

        # replace 'NA' or "." by None
        if numeric_cols:
            chunk[numeric_cols] = chunk[numeric_cols].replace({".": None})

        chunk = chunk.replace({np.nan: None})
        # do field val replacements if given
        if replacements:
            chunk = chunk.replace(replacements)

        # normalize the gene col
        if settings.replace_ensembl_ids_in_db:
            chunk["gene"] = chunk["gene"].apply(normalize_gene)

        # convert the rows into a seq of dicts
        data_chunk = chunk.to_dict("records")

        yield data_chunk


def file_chunker_pandas_with_split(
    file_obj,
    split_column=None,
    split_delimiter=",",
    explode_split=False,
    dtypes=None,
    chunksize=100,
    sep="\t",
    lower_headers=False,
    skiprows=0,
    replacements=None,
    numeric_cols=None,
    **kwargs,
):
    # Adjust chunksize if exploding (since it multiplies rows)
    actual_chunksize = chunksize // 3 if explode_split and split_column else chunksize

    for chunk in pd.read_csv(
        file_obj,
        dtype=dtypes,
        skiprows=skiprows,
        chunksize=actual_chunksize,
        delimiter=sep,
    ):
        # lower case the headers
        if lower_headers:
            chunk.columns = [col.lower() for col in chunk.columns]
            if split_column:
                split_column = split_column.lower()

        # replace 'NA' or "." by None
        if numeric_cols:
            chunk[numeric_cols] = chunk[numeric_cols].replace({".": None})

        chunk = chunk.replace({np.nan: None})

        # do field val replacements if given
        if replacements:
            chunk = chunk.replace(replacements)

        # normalize the gene col
        if settings.replace_ensembl_ids_in_db:
            chunk["gene"] = chunk["gene"].apply(normalize_gene)

        # String splitting and exploding
        if split_column and split_column in chunk.columns:
            # Split the column
            chunk[f"{split_column}_split"] = chunk[split_column].str.split(
                split_delimiter
            )

            # Explode if requested
            if explode_split:
                chunk = chunk.explode(f"{split_column}_split")
                # Reset index after exploding
                chunk = chunk.reset_index(drop=True)

        # convert the rows into a seq of dicts
        data_chunk = chunk.to_dict("records")

        yield data_chunk


def file_chunker_polars(
    file_path,
    split_columns=None,
    split_delimiter=", ",
    explode_split=False,
    exploded_column="",  # the new col name to hold the exploded value needed
    dtypes=None,
    chunksize=1000,
    sep="\t",
    lower_headers=False,
    skiprows=0,
    replacements=None,
    hash_columns=None,
    hash_column_name="variant_hash",
    numeric_cols=None,
    **kwargs,
):
    """
    Optimized for string splitting operations on large files
    Exploding happens immediately after splitting if requested
    """

    def process_chunk_with_split(
        chunk_df,
        split_columns=None,
        split_delimiter=", ",
        should_explode=False,
        exploded_col="",
        hash_columns=None,
        hash_column_name="variant_hash",
    ):
        """Apply all transformations including string splitting to a chunk"""
        df = chunk_df

        # Apply your existing transformations first
        if lower_headers:
            df = df.rename({col: col.lower() for col in df.columns})
            if split_columns:
                split_columns = (
                    col.lower() for col in split_columns
                )  # Update split column name if needed

        # Replace '.' with None in numeric columns: not needed since loading already replaced . with None
        """
        if numeric_cols:
            transformations = []
            for col in numeric_cols:
                if col in df.columns:
                    transformations.append(
                        pl.when(pl.col(col) == ".")
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )

            if transformations:
                other_cols = [
                    pl.col(col) for col in df.columns if col not in numeric_cols
                ]
                df = df.select(transformations + other_cols)
        """

        # Do field value replacements
        if replacements:
            transformations = []
            for col in df.columns:
                col_expr = pl.col(col)
                for old_val, new_val in replacements.items():
                    col_expr = (
                        pl.when(col_expr == old_val).then(new_val).otherwise(col_expr)
                    )
                transformations.append(col_expr.alias(col))
            df = df.select(transformations)

        # Normalize gene column
        if (
            hasattr(settings, "replace_ensembl_ids_in_db")
            and settings.replace_ensembl_ids_in_db
            and "gene" in df.columns
        ):
            df = df.with_columns(
                [pl.col("gene").map_elements(normalize_gene, return_dtype=pl.Utf8)]
            )

        # String splitting operation
        for split_col in split_columns:
            if split_col in df.columns:
                if not should_explode:
                    df = df.with_columns(
                        pl.col(split_col).str.to_uppercase().str.split(split_delimiter)
                    )

                else:
                    df = df.with_columns(
                        pl.col(split_col)
                        .str.to_uppercase()
                        .str.split(split_delimiter)
                        .alias(exploded_col)
                    )

                    # Explode immediately if requested
                    if should_explode:
                        if not exploded_col:
                            exploded_col = f"{split_col}_split"

                        df = df.explode(exploded_col)

                        # Optionally drop the original column to save memory
                        # df = df.drop(split_col)  # Uncomment to save memory
        if hash_columns:
            # add the hash col
            df = add_hash_to_chunk(
                df, hash_columns=hash_columns, hash_column_name=hash_column_name
            )

            # dedupe
            # Remove duplicates if needed
            df_unique = df.unique(subset=[hash_column_name], keep="last")

            if len(df) > len(df_unique):
                print(
                    f"WARNING: there are {len(df) - len(df_unique)} duplicates in the chunk"
                )
                df = df_unique

        return df

    # Adjust batch size based on whether we're exploding
    if explode_split:
        batch_size = max(chunksize * 2, 1000)  # Smaller batches for explode
        final_chunksize = (
            chunksize // 2
        )  # Smaller final chunks since explode increases rows
    else:
        batch_size = chunksize
        final_chunksize = chunksize

    lazy_df = pl.scan_csv(
        file_path,
        separator=sep,
        skip_rows=skiprows,
        schema_overrides=dtypes,
        null_values=["", "NA", "."],
    )

    offset = 0
    while True:
        # Get a batch
        batch_lazy = lazy_df.slice(offset, batch_size)

        try:
            batch = batch_lazy.collect()
        except Exception as e:
            print(f"Error processing batch at offset {offset}: {e}")
            break

        if len(batch) == 0:
            break

        # Apply transformations including string splitting and optional exploding
        try:
            processed_batch = process_chunk_with_split(
                batch,
                split_columns=split_columns,
                split_delimiter=split_delimiter,
                should_explode=explode_split,
                exploded_col=exploded_column,
                hash_columns=hash_columns,
                hash_column_name=hash_column_name,
            )
        except Exception as e:
            print(f"Error processing transformations at offset {offset}: {e}")
            offset += batch_size
            continue

        # Subdivide into smaller chunks for downstream processing
        batch_len = len(processed_batch)
        for chunk_start in range(0, batch_len, final_chunksize):
            chunk_end = min(chunk_start + final_chunksize, batch_len)
            chunk = processed_batch.slice(chunk_start, chunk_end - chunk_start)

            # Yield the DataFrame directly instead of converting to dict
            yield chunk.to_dicts()

        offset += batch_size

        if len(batch) < batch_size:
            break


# Memory-optimized version that handles multiple split columns
def file_chunker_multi_split(
    file_path,
    split_columns,  # Dict: {"col_name": "delimiter"}
    explode_columns=None,  # List of columns to explode
    chunksize=500,
    **kwargs,
):
    lower_headers = kwargs.get("lower_headers")
    replacements = kwargs.get("replacements")
    numeric_cols = kwargs.get("numeric_cols")

    """
    Handle multiple string splitting operations efficiently
    """
    explode_columns = explode_columns or []

    def process_chunk_multi_split(chunk_df):
        df = chunk_df

        # Apply all your existing transformations first
        if lower_headers:
            df = df.rename({col: col.lower() for col in df.columns})

        # Replace '.' with None in numeric columns
        if numeric_cols:
            transformations = []
            for col in numeric_cols:
                if col in df.columns:
                    transformations.append(
                        pl.when(pl.col(col) == ".")
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )

            if transformations:
                other_cols = [
                    pl.col(col) for col in df.columns if col not in numeric_cols
                ]
                df = df.select(transformations + other_cols)

        # Do field value replacements
        if replacements:
            transformations = []
            for col in df.columns:
                col_expr = pl.col(col)
                for old_val, new_val in replacements.items():
                    col_expr = (
                        pl.when(col_expr == old_val).then(new_val).otherwise(col_expr)
                    )
                transformations.append(col_expr.alias(col))
            df = df.select(transformations)

        # Normalize gene column
        if (
            hasattr(settings, "replace_ensembl_ids_in_db")
            and settings.replace_ensembl_ids_in_db
            and "gene" in df.columns
        ):
            df = df.with_columns(
                [pl.col("gene").map_elements(normalize_gene, return_dtype=pl.Utf8)]
            )

        # Apply all string splits
        split_transformations = []
        for col, delimiter in split_columns.items():
            if col in df.columns:
                split_transformations.append(
                    pl.col(col).str.split(delimiter).alias(f"{col}_split")
                )

        if split_transformations:
            df = df.with_columns(split_transformations)

        # Explode specified columns
        for col in explode_columns:
            split_col_name = f"{col}_split"
            if split_col_name in df.columns:
                df = df.explode(split_col_name)

        return df

    # Process with smaller batches due to potential multiple explodes
    batch_size = max(chunksize * 2, 800)

    lazy_df = pl.scan_csv(
        file_path,
        separator=kwargs.get("sep", "\t"),
        skip_rows=kwargs.get("skiprows", 0),
        dtypes=kwargs.get("dtype"),
        null_values=["", "NA", "."],
    )

    offset = 0
    while True:
        batch_lazy = lazy_df.slice(offset, batch_size)
        batch = batch_lazy.collect()

        if len(batch) == 0:
            break

        processed_batch = process_chunk_multi_split(batch)

        # Yield in smaller chunks
        batch_len = len(processed_batch)
        for chunk_start in range(0, batch_len, chunksize):
            chunk_end = min(chunk_start + chunksize, batch_len)
            chunk = processed_batch.slice(chunk_start, chunk_end - chunk_start)
            yield chunk

        offset += batch_size
        if len(batch) < batch_size:
            break


def convert_filter_to_sql_where_clause_simple(filter_spec, partition_col=None):
    constraints = []
    for name, filter in filter_spec.items():
        name = name.lower()
        if name == partition_col:
            continue
        if isinstance(filter, dict):
            # it's a range of the form {">", 1, "<=": 10}

            cons_clauses = [
                f"{name} {OP_MAPPINGS.get(op)} {val}" for op, val in filter.items()
            ]
            clause = " AND ".join(cons_clauses)
            constraints.append(clause)

        elif isinstance(filter, list):
            # construct boolean or
            constraints.append(" OR ".join([f"{name} = '{val}'" for val in filter]))
        elif isinstance(filter, str):
            # add single quotes
            constraints.append(f"{name} = '{filter}'")
        elif isinstance(filter, int) or isinstance(filter, float):
            constraints.append(f"{name} = {filter}")
        else:
            raise ValueError(f"Invalid filter: {filter}")

    if constraints:
        return f" WHERE {' AND '.join(constraints)}"

    return ""


def convert_filter_to_sql_where_clause(
    filter_spec, partition_col=None, array_columns=None
):
    """
    Convert filter specifications to SQL WHERE clause, with support for ARRAY columns.

    :param filter_spec: Dictionary of filters
    :param partition_col: Column to skip (partition column)
    :param array_columns: List of column names that are ARRAY types (e.g., ['gene'])
    :return: SQL WHERE clause string
    """
    array_columns = array_columns or ["gene"]  # Default gene column as array
    constraints = []

    if not filter_spec:
        return ""

    for name, filter_value in filter_spec.items():
        name = name.lower()
        if name == partition_col:
            continue

        # Check if this is an array column
        is_array_column = name in array_columns

        if isinstance(filter_value, dict):
            # Handle range filters like {">": 1, "<=": 10}
            cons_clauses = [
                f"{name} {OP_MAPPINGS.get(op)} {val}"
                for op, val in filter_value.items()
            ]
            clause = " AND ".join(cons_clauses)
            constraints.append(clause)

        elif isinstance(filter_value, list):
            if is_array_column:
                # Alternative: Use ANY operator (more type-flexible)
                conditions = [f"'{val}' = ANY({name})" for val in filter_value]
                if len(conditions) == 1:
                    constraints.append(conditions[0])
                else:
                    # For multiple values, check if any of them exist
                    constraints.append(f"({' OR '.join(conditions)})")
            else:
                # Regular IN clause for non-array columns
                if all(isinstance(v, str) for v in filter_value):
                    values = "', '".join(filter_value)
                    constraints.append(f"{name} IN ('{values}')")
                else:
                    values = ", ".join(str(v) for v in filter_value)
                    constraints.append(f"{name} IN ({values})")

        elif isinstance(filter_value, str):
            if is_array_column:
                # Use ANY operator - no casting needed
                constraints.append(f"'{filter_value}' = ANY({name})")
            else:
                # Regular string equality
                constraints.append(f"{name} = '{filter_value}'")

        elif isinstance(filter_value, (int, float)):
            if is_array_column:
                # Convert number to string for array search
                constraints.append(f"'{filter_value}' = ANY({name})")
            else:
                # Regular numeric equality
                constraints.append(f"{name} = {filter_value}")

        else:
            raise ValueError(f"Invalid filter: {filter_value}")

    if constraints:
        return f" WHERE {' AND '.join(constraints)}"

    return ""


def convert_filter_to_sql_where_clause_wrong(
    filter_spec, partition_col=None, array_columns=None
):
    """
    Convert filter specifications to SQL WHERE clause, with support for ARRAY columns.

    :param filter_spec: Dictionary of filters
    :param partition_col: Column to skip (partition column)
    :param array_columns: List of column names that are ARRAY types (e.g., ['gene'])
    :return: SQL WHERE clause string
    """
    array_columns = array_columns or ["gene"]  # Default gene column as array
    constraints = []

    if not filter_spec:
        return ""

    for name, filter_value in filter_spec.items():
        name = name.lower()
        if name == partition_col:
            continue

        # Check if this is an array column
        is_array_column = name in array_columns

        if isinstance(filter_value, dict):
            # Handle range filters like {">": 1, "<=": 10}
            # it's a range of the form {">", 1, "<=": 10}

            cons_clauses = [
                f"{name} {OP_MAPPINGS.get(op)} {val}"
                for op, val in filter_value.items()
            ]
            clause = " AND ".join(cons_clauses)
            constraints.append(clause)

        elif isinstance(filter_value, list):
            if is_array_column:
                # For array columns, check if array contains ANY or ALL of the specified values
                # Default to ANY (overlap operator &&)
                escaped_values = [f"'{val}'" for val in filter_value]
                array_literal = "{" + ",".join(escaped_values) + "}"
                constraints.append(f"{name} && ARRAY[{array_literal}]")
            else:
                # Regular IN clause for non-array columns
                if all(isinstance(v, str) for v in filter_value):
                    values = "', '".join(filter_value)
                    constraints.append(f"{name} IN ('{values}')")
                else:
                    values = ", ".join(str(v) for v in filter_value)
                    constraints.append(f"{name} IN ({values})")

        elif isinstance(filter_value, str):
            if is_array_column:
                # Check if array contains the specific string
                constraints.append(f"{name} @> ARRAY['{filter_value}']")
            else:
                # Regular string equality
                constraints.append(f"{name} = '{filter_value}'")

        elif isinstance(filter_value, (int, float)):
            if is_array_column:
                # Convert number to string for array search
                constraints.append(f"{name} @> ARRAY['{filter_value}']")
            else:
                # Regular numeric equality
                constraints.append(f"{name} = {filter_value}")

        else:
            raise ValueError(f"Invalid filter: {filter_value}")

    if constraints:
        return f" WHERE {' AND '.join(constraints)}"

    return ""


def get_gene_symbols(ensembl_ids):
    global MG_CLIENT
    if not MG_CLIENT:
        MG_CLIENT = mygene.MyGeneInfo()

    if isinstance(ensembl_ids, str):
        gene_info = MG_CLIENT.getgene(ensembl_ids)
        return gene_info.get("symbol", "")
        # sym = get_gene_symbol_ensembl(ensembl_ids)
        # if sym and not sym.startswith('LOC'):
        #    return {ensembl_ids: sym}
    else:
        # results = mg.querymany(ensembl_ids, scopes='ensembl.gene', fields='symbol', species='human')
        results = MG_CLIENT.getgenes(ensembl_ids)

        gene_symbols = {}
        for result in results:
            sym = result.get("symbol")
            if sym and not sym.startswith("LOC"):
                gene_symbols[result.get("query")] = sym
        return gene_symbols


def get_gene_symbol_ensembl(ensembl_id):
    server = "https://rest.ensembl.org"
    endpoint = f"/lookup/id/{ensembl_id}?content-type=application/json"
    response = requests.get(server + endpoint)

    if not response.ok:
        # response.raise_for_status()
        return None

    data = response.json()
    return data.get("display_name", None)


def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()

        print("Total time running %s: %s seconds" % (function.__name__, str(t1 - t0)))

        return result

    return function_timer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--debug", type=int, default=0)
    parser.add_argument("-f", "--filters", help="the json string of filters")
    parser.add_argument(
        "-c", "--chunksize", type=int, default=100, help="the json string of filters"
    )
    parser.add_argument(
        "--skip_rows", type=int, default=0, help="The number of rows to skip"
    )
    parser.add_argument(
        "--ensembl_to_symbol", type=int, help="convert the ensembl ids to gene symbols"
    )

    parser.add_argument(
        "--chunker_type",
        default="polars",
        help="use the type of chunker to create chunks of the data file. Options: polars, pandas",
    )
    parser.add_argument("--data_file")

    options = parser.parse_args()

    if options.debug > 1:
        ipdb.set_trace()

    if options.filters:
        filters = json.loads(options.filters)
        print(convert_filter_to_sql_where_clause(filters))

    elif options.data_file:
        with open(options.data_file) as file_obj:
            if options.ensembl_to_symbol:
                cnt = 0
                chunk = []
                all_mappings = {}
                for line in file_obj:
                    line = line.strip()
                    cnt += 1
                    if cnt <= options.skip_rows:
                        continue

                    chunk.append(line)
                    if len(chunk) == options.chunksize:
                        sym_maps = get_gene_symbols(chunk)
                        if sym_maps:
                            all_mappings.update(sym_maps)
                        chunk = []

                if chunk:
                    sym_maps = get_gene_symbols(chunk)
                    all_mappings.update(sym_maps)

                # write out the mappings in json
                with open("ensembel_id_to_gene_symbol.json", "w") as mf:
                    # Dump the JSON object to the file
                    json.dump(all_mappings, mf)
            else:
                if options.chunker_type == "pandas":
                    chunker_func = file_chunker_pandas
                elif options.chunker_type == "polars":
                    chunker_func = file_chunker_polars
                else:
                    raise ValueError(f"Unknown chunker type: {options.chunker_type}")
                chunker = chunker_func(
                    file_obj,
                    chunksize=options.chunksize,
                    lower_headers=True,
                    skiprows=options.skip_rows,
                )

                for chunk in chunker:
                    print(chunk)
