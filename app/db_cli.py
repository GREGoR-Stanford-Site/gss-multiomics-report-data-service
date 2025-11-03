import argparse
import ipdb
import os
import json
from app.db_manager import GSS_Database, initialize_db, close_db
from app.config import settings


# Dependency to get the database instance
def get_db(db_name: str = settings.db_name) -> GSS_Database:
    return initialize_db(db_name=db_name)


def json_type(json_string):
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        raise argparse.ArgumentTypeError(f"Invalid JSON string: '{json_string}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--debug", type=int, default=0)
    parser.add_argument(
        "--host",
        help="the hostname: options: db (inside the container), localhost(outside the container locally",
    )
    parser.add_argument(
        "-n",
        "--db_name",
        default=settings.db_name,
        help="The database name to connect to",
    )
    parser.add_argument("--create_tables", nargs="*")
    parser.add_argument("--drop_tables", nargs="*")
    parser.add_argument("--chunk_size", type=int, default=1000)
    parser.add_argument("--skiprows", type=int, default=0)
    parser.add_argument("--start_chunk", type=int, help="The chunk index to load")
    parser.add_argument("--chunk_count", type=int, help="How many chunks to load")
    parser.add_argument(
        "--skip_table_name_pat", help="The regex pattern for the chroms data to skip"
    )
    parser.add_argument(
        "--valid_table_name_pat", help="The regex pattern for the chroms data to load"
    )
    parser.add_argument(
        "--update_col_name", help="The name of the column to be updated"
    )
    parser.add_argument(
        "--replacements",
        help="a json file containing mappings of fields in lower case to dict of old->new values",
    )

    parser.add_argument("--data_files", nargs="*", help="a list of data files to load")

    parser.add_argument(
        "--selected_fields", nargs="*", help="a list of field names for query select"
    )

    parser.add_argument(
        "--filters",
        type=json_type,
        help="A json string of a dict for filtering the data",
    )

    parser.add_argument(
        "--size", type=int, help="the number of results for the limit in the query"
    )

    options = parser.parse_args()

    if options.debug > 1:
        ipdb.set_trace()

    db = get_db(db_name=options.db_name)
    if not db:
        raise Exception(f"Invalid db_name: {options.db_name}")

    params = vars(options)

    if options.create_tables:
        db.create_gsf_tables(options.create_tables, db_name=options.db_name)
    elif options.drop_tables:
        for table in options.drop_tables:
            db._drop_table(table)
    elif options.data_files:
        for data_file in options.data_files:
            if os.path.exists(data_file):
                if options.update_col_name:
                    # the data file should be a json of old->new mappings
                    val_mappings = json.load(open(data_file))

                    db.update_col_values(
                        options.update_col_name, val_mappings, db_name=options.db_name
                    )

                else:
                    print(f"Loading data file: {data_file}")
                    db.load(data_file, **params)

    else:
        # for queries
        if options.size or options.filters:
            try:
                with db.get_session() as session:
                    result = db.search(session, **params)
                    print(result)
            except Exception as e:
                raise ValueError(f"Search error: {str(e)}")

    # cleanup
    close_db()
