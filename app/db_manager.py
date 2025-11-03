from contextlib import contextmanager
from sqlalchemy import (
    MetaData,
    Table,
    create_engine,
    Column,
    update,
    Index,
    Float,
    Integer,
    String,
    Boolean,
    text,
    UniqueConstraint,
    CheckConstraint,
    ForeignKey,
    inspect,
)

from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects.postgresql import insert as pg_insert, ARRAY
from typing import Dict, Any

from collections import defaultdict
import re
import json
import logging

from google.cloud.sql.connector import Connector, IPTypes

from app.config import settings
from app.utils import (
    convert_filter_to_sql_where_clause,
    file_chunker_pandas,
    file_chunker_polars,
    fn_timer,
)

TO_PYTHON_TYPES = {
    "string": str,
    "integer": int,
    "float": float,
    "boolean": bool,
    "array": str,
}

TO_SQLALCHEMY_TYPES = {
    "string": String,
    "integer": Integer,
    "float": Float,
    "boolean": Boolean,
}

GENE_DELIM_REGX = re.compile(r"[,\s]+")
GENE2CHROM_UNIQUE_KEY_NAMES = ("gene", "chrom")

UNIQUE_COLS = {
    "gss-multiomics-report": ("family_id", "chrom", "start", "end", "ref", "alt"),
    "gss_data": ("pos", "ref", "alt", "gene"),
}

default_table_schema_map = {
    "gss_data": {
        "chrom": {"type": "string"},
        "pos": {"type": "integer", "is_index": True},
        "ref": {"type": "string"},
        "alt": {"type": "string"},
        "ac": {"type": "integer"},
        "an": {"type": "integer"},
        "af": {"type": "float"},
        "filter": {"type": "string"},
        "ac_hemi": {"type": "integer"},
        "ac_het": {"type": "integer"},
        "ac_hom": {"type": "integer"},
        "f_missing": {"type": "float"},
        "type": {"type": "string"},
        "gene": {"type": "string", "is_index": True, "index_type": "hash"},
        "consequence": {"type": "string"},
        "vrs_id": {"type": "string"},
    },
    "gss-multiomics-report": {
        "family_id": {"type": "string", "is_index": True},
        "chrom": {"type": "string", "is_index": True},
        "start": {"type": "integer", "is_index": True},
        "end": {"type": "integer", "is_index": True},
        "ref": {"type": "string"},
        "alt": {"type": "string"},
        "length": {"type": "integer"},
        "type": {"type": "string"},
        "genotype": {"type": "string"},
        "maternal_genotype": {"type": "string"},
        "paternal_genotype": {"type": "string"},
        "inheritance": {"type": "string"},
        "af_all": {"type": "float"},
        "af_afr": {"type": "float"},
        "af_amr": {"type": "float"},
        "af_eas": {"type": "float"},
        "af_eur": {"type": "float"},
        "af_sas": {"type": "float"},
        "omim": {"type": "string"},
        "exonic": {"type": "boolean"},
        "centromeric": {"type": "boolean"},
        "pericentromeric": {"type": "boolean"},
        "telomeric": {"type": "boolean"},
        "str": {"type": "boolean"},
        "vntr": {"type": "boolean"},
        "segdup": {"type": "boolean"},
        "repeat": {"type": "boolean"},
        "gap": {"type": "boolean"},
        "hiconf": {"type": "boolean"},
        "alt_reads": {"type": "integer"},
        "ref_reads": {"type": "integer"},
        "total_reads": {"type": "integer"},
        "gt_homwt": {"type": "integer"},
        "gt_het": {"type": "integer"},
        "gt_homvar": {"type": "integer"},
        # "gene_key": {"type": "string", "is_index": True, "index_type": "hash"},
        "gene": {"type": "array", "element_type": "string"},
        "variant_hash": {"type": "string"},  # Will store MD5 hash
        # Multi-column indexes
        "_indexes": [
            {
                "name": "genomic_position",
                "columns": ["chrom", "start", "end"],
                "type": "btree",
            },
            {
                "name": "family_position",
                "columns": ["family_id", "chrom", "start"],
                "type": "btree",
            },
            {
                "name": "unique_sv_hash",  # New unique index
                "columns": ["variant_hash"],
                "type": "btree",
                "unique": True,
            },
            {"name": "gene_gin", "columns": ["gene"], "type": "gin"},
        ],
    },
}

# a global logger
LOGGER = logging.getLogger(__name__)

# Global variables for application-wide state
connector = None
engine = None


# a global connector instance
def create_connector():
    return Connector(
        ip_type=IPTypes.PUBLIC,  # or IPTypes.PRIVATE for localhost
        refresh_strategy="lazy",  # or "background"
        timeout=30,
    )


# Function to create a SQLAlchemy engine using Cloud SQL Connector
def create_db_engine_with_connector(
    connection_name: str, connector: Connector, db_user: str, db_pass: str, db_name: str
):
    def getconn():
        return connector.connect(
            connection_name,
            "pg8000",  # Replace with "pymysql" for MySQL
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=IPTypes.PUBLIC,  # Use IPTypes.PRIVATE for private IP
        )

    # Create the SQLAlchemy engine using the 'creator' argument
    engine = create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )
    return engine


def create_db_engine(
    db_url: str = None,
    connector=None,
    connection_name: str = None,
    db_user: str = None,
    db_pass: str = None,
    db_name: str = None,
):
    if connector and connection_name and db_user and db_pass and db_name:
        return create_db_engine_with_connector(
            connection_name=connection_name,
            connector=connector,
            db_user=db_user,
            db_pass=db_pass,
            db_name=db_name,
        )
    elif db_url:
        return create_engine(db_url)
    else:
        raise ValueError("Insufficient parameters to create database engine.")


def initialize_db(db_name: str = settings.db_name):
    """Initializes the global connector and engine."""
    global connector, engine

    if settings.use_gcp_db and connector is None:
        # create a connector
        connector = create_connector()

    if engine is None:
        engine = create_db_engine(
            db_url=settings.db_urls[db_name],
            connector=connector,
            connection_name=settings.instance_connection_name,
            db_user=settings.postgres_user,
            db_pass=settings.postgres_password,
            db_name=db_name,
        )

    return SV_Database(db_name, engine=engine)


def close_db():
    """Cleans up resources. Safe to call multiple times."""
    global connector, engine
    if engine:
        engine.dispose()
        engine = None
    if connector:
        connector.close()
        connector = None


def create_dynamic_table(
    table_name: str,
    metadata: MetaData,
    schema: Dict[str, Any],
    skip_auto_pk: bool = False,
) -> Table:
    """
    Dynamically creates a SQLAlchemy Table object from a schema dictionary.

    :param table_name: Name of the table to create.
    :param metadata: SQLAlchemy Metadata object.
    :param schema: Dictionary representing the schema of the table.
    :param skip_auto_pk: If True, don't add automatic primary key (useful when schema defines its own PK)
    :return: SQLAlchemy Table object.
    """
    columns = []
    indexes = []

    # Check if schema defines its own primary key
    has_explicit_pk = any(
        col_props.get("primary_key", False) or col_props.get("is_primary", False)
        for col_props in schema.values()
        if isinstance(col_props, dict)
    )

    # Add an autoincrementing primary key only if not explicitly defined and not skipped
    if not has_explicit_pk and not skip_auto_pk:
        columns.append(Column("id", Integer, primary_key=True, autoincrement=True))

    # Process regular columns
    for col_name, col_props in schema.items():
        # Skip special keys that start with underscore (like _indexes)
        if col_name.startswith("_"):
            continue

        # Skip if this is the chrom column and it's used for table naming
        if col_name == "chrom" and table_name.startswith("chrom_"):
            continue

        col_type_str = col_props.get("type", "string")
        col_type = TO_SQLALCHEMY_TYPES.get(col_type_str, String)

        # Handle column-specific properties
        is_index = col_props.get("is_index", False)
        is_primary_key = col_props.get("primary_key", False) or col_props.get(
            "is_primary", False
        )
        is_autoincrement = col_props.get("autoincrement", False)  # Added this line
        foreign_key_info = col_props.get("foreign_key")
        index_type = col_props.get("index_type")

        column_args = []
        column_kwargs = {}

        # Handle primary key and autoincrement
        if is_primary_key:
            column_kwargs["primary_key"] = True
            # Handle autoincrement: explicit setting or default for integer PKs
            if is_autoincrement or (
                col_type_str == "integer"
                and col_props.get("autoincrement") is not False
            ):
                column_kwargs["autoincrement"] = True

        # Handle foreign key
        if foreign_key_info:
            ref_table = foreign_key_info["table"]
            ref_column = foreign_key_info["column"]
            ondelete = foreign_key_info.get("ondelete")

            if ondelete:
                fk = ForeignKey(f"{ref_table}.{ref_column}", ondelete=ondelete)
            else:
                fk = ForeignKey(f"{ref_table}.{ref_column}")

            column_args.append(fk)

        # Handle single-column indexes (but not hash indexes, those are handled separately)
        if is_index and index_type != "hash":
            column_kwargs["index"] = True

        # Create the column
        columns.append(Column(col_name, col_type, *column_args, **column_kwargs))

        # Handle single-column hash indexes
        if is_index and index_type == "hash":
            indexes.append(
                Index(f"ix_{table_name}_{col_name}", col_name, postgresql_using="hash")
            )

    # Process multi-column indexes
    multi_indexes = schema.get("_indexes", [])
    for index_def in multi_indexes:
        index_name = index_def["name"]
        index_columns = index_def["columns"]
        index_type = index_def.get("type", "btree")
        is_unique = index_def.get("unique", False)
        where_clause = index_def.get("where")

        # Build index arguments
        index_kwargs = {}
        if index_type == "hash":
            index_kwargs["postgresql_using"] = "hash"
        if is_unique:
            index_kwargs["unique"] = True
        if where_clause:
            index_kwargs["postgresql_where"] = text(where_clause)

        indexes.append(
            Index(f"ix_{table_name}_{index_name}", *index_columns, **index_kwargs)
        )

    return Table(table_name, metadata, *columns, *indexes)


class GSS_Database:
    def __init__(
        self,
        db_name,
        partition_col=None,
        gene_col="gene",
        table_name=None,
        engine=None,
        **kwargs,
    ):
        self.db_name = db_name
        self.partition_col = partition_col
        self.gene_col = gene_col
        if table_name:
            self.default_table_name = table_name
        else:
            self.default_table_name = "all_chroms" if not partition_col else None
        self.engine = engine
        self.metadata = MetaData()
        self.Base = declarative_base(metadata=self.metadata)
        self.inspector = Inspector.from_engine(self.engine)
        self.valid_table_names = set()
        self.models = {}
        self.default_table_schema = default_table_schema_map.get(db_name, {})
        self.default_numeric_col_names = [
            key
            for key, val in self.default_table_schema.items()
            if (not key.startswith("_"))
            and isinstance(val, dict)
            and val.get("type") != "string"
        ]
        self.default_table_col_python_types = {
            key: TO_PYTHON_TYPES[val["type"]]
            for key, val in self.default_table_schema.items()
            if not key.startswith("_") and isinstance(val, dict)
        }
        self.default_primary_key_names = [
            key
            for key, val in self.default_table_schema.items()
            if not key.startswith("_")
            and isinstance(val, dict)
            and val.get("is_primary")
        ]

        self.default_unique_index_key_names = UNIQUE_COLS.get(self.db_name)
        # if set will create a hash col using the unique cols
        self.hash_column_name = ""
        self.get_curr_table_names()

        # 3. Use metadata.create_all() to ensure all tables defined on
        #    self.Base (including 'gene2chrom') exist in the database.
        #    checkfirst=True prevents errors if they already exist.
        self.metadata.create_all(self.engine, checkfirst=True)

    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        :param table_name: Name of the table to check
        :return: True if table exists, False otherwise
        """
        try:
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()
            return table_name in existing_tables
        except Exception as e:
            print(f"Error checking if table exists: {e}")
            return False

    @contextmanager
    def get_session(self):
        """Provide a transactional scope around a series of operations."""
        session = Session(bind=self.engine.connect())
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_curr_table_names(self):
        self.valid_table_names = set(self.inspector.get_table_names())
        table_names_str = ", ".join(sorted(self.valid_table_names))
        print(f"Current table names in {self.db_name}: {table_names_str}")
        return self.valid_table_names

    def create_gsf_tables(self, table_names):
        try:
            for table_name in table_names:
                table_obj = self.create_dynamic_table(
                    table_name, self.metadata, self.default_table_schema
                )

                # Create the table DLL and send to the db if it doesn't exist
                if not table_obj.exists(self.engine):
                    table_obj.create(self.engine)

                self.valid_table_names[self.db_name].add(table_name)

                self.models[self.db_name][table_name] = table_obj
        except Exception as e:
            return {
                f"Error creating the table for {table_name} in {self.db_name}": str(e)
            }

    def create_gsf_model_simple(self, table_name, **columns):
        attrs = {
            "__tablename__": table_name,
            "id": Column(Integer, primary_key=True, autoincrement=True),
        }

        col_defs = columns or self.default_table_schema

        hash_index_names = []
        for name, col_def in col_defs.items():
            if name == self.partition_col:
                continue

            stype = TO_SQLALCHEMY_TYPES[col_def.get("type", "string")]
            if col_def.get("is_primary"):
                attrs[name] = Column(stype, primary_key=True)
            elif col_def.get("is_index"):
                index_type = col_def.get("index_type", "")
                if index_type == "hash":
                    attrs[name] = Column(stype)
                    hash_index_names.append(name)
                else:
                    attrs[name] = Column(stype, index=True)
            else:
                attrs[name] = Column(stype)

        if hash_index_names:
            attrs["__table_args__"] = tuple(
                [
                    Index(f"ix_{table_name}_{name}", name, postgresql_using="hash")
                    for name in hash_index_names
                ]
            )

        return type(table_name, (self.Base,), attrs)

    def create_gsf_model(self, table_name, **columns):
        """
        Create a SQLAlchemy model class dynamically from schema definition.

        :param table_name: Name of the table/model
        :param columns: Schema definition dictionary
        :return: SQLAlchemy model class
        """
        attrs = {
            "__tablename__": table_name,
        }

        col_defs = columns or self.default_table_schema

        # Lists to collect different types of indexes and constraints
        hash_index_names = []
        table_args = []
        gin_index_names = []

        # Check if schema defines its own primary key
        has_explicit_pk = any(
            col_def.get("is_primary", False) or col_def.get("primary_key", False)
            for col_def in col_defs.values()
            if isinstance(col_def, dict)
        )

        # Add auto-incrementing ID only if no explicit primary key is defined
        if not has_explicit_pk:
            attrs["id"] = Column(Integer, primary_key=True, autoincrement=True)

        # Process regular columns
        for name, col_def in col_defs.items():
            # Skip special keys that start with underscore (like _indexes)
            if name.startswith("_"):
                continue

            # Skip partition column
            # if name == self.partition_col:
            #    continue

            # Get column type
            col_type_str = col_def.get("type", "string")

            # Handle array columns
            if col_type_str == "array":
                element_type_str = col_def.get("element_type", "string")
                element_type = TO_SQLALCHEMY_TYPES.get(element_type_str, String)
                col_type = ARRAY(element_type)
            else:
                col_type = TO_SQLALCHEMY_TYPES.get(col_type_str, String)

            # stype = TO_SQLALCHEMY_TYPES.get(col_type_str, String)

            # Handle column properties
            is_primary = col_def.get("is_primary", False) or col_def.get(
                "primary_key", False
            )
            is_autoincrement = col_def.get("autoincrement", False)  # Added this line
            is_index = col_def.get("is_index", False)
            index_type = col_def.get("index_type", "")
            foreign_key_info = col_def.get("foreign_key")

            # Create column arguments
            column_args = []
            column_kwargs = {}

            # Handle primary key and autoincrement
            if is_primary:
                column_kwargs["primary_key"] = True
                # Auto-increment for integer primary keys or explicitly requested
                if col_type_str == "integer" and (
                    is_autoincrement or col_def.get("autoincrement")
                ):
                    column_kwargs["autoincrement"] = True
                elif is_autoincrement:
                    column_kwargs["autoincrement"] = True

            # Handle foreign key
            if foreign_key_info:
                ref_table = foreign_key_info["table"]
                ref_column = foreign_key_info["column"]
                ondelete = foreign_key_info.get("ondelete")

                if ondelete:
                    fk = ForeignKey(f"{ref_table}.{ref_column}", ondelete=ondelete)
                else:
                    fk = ForeignKey(f"{ref_table}.{ref_column}")

                column_args.append(fk)

            # Handle single-column indexes
            if is_index:
                if index_type == "hash":
                    # Hash indexes are handled separately in __table_args__
                    hash_index_names.append(name)
                elif index_type == "gin":
                    gin_index_names.append(name)
                else:
                    # Regular B-tree index
                    column_kwargs["index"] = True

            # Create the column
            attrs[name] = Column(col_type, *column_args, **column_kwargs)

        # Process single-column hash indexes
        if hash_index_names:
            for name in hash_index_names:
                table_args.append(
                    Index(f"ix_{table_name}_{name}_hash", name, postgresql_using="hash")
                )

        if gin_index_names:
            for name in gin_index_names:
                table_args.append(
                    Index(f"ix_{table_name}_{name}_gin", name, postgresql_using="gin")
                )

        # Process multi-column indexes
        multi_indexes = col_defs.get("_indexes", [])
        for index_def in multi_indexes:
            index_name = index_def["name"]
            index_columns = index_def["columns"]
            index_type = index_def.get("type", "btree")
            is_unique = index_def.get("unique", False)
            where_clause = index_def.get("where")

            # Build index arguments
            index_kwargs = {}
            if index_type == "hash":
                index_kwargs["postgresql_using"] = "hash"
            elif index_type == "gin":
                index_kwargs["postgresql_using"] = "gin"
            elif is_unique:
                index_kwargs["unique"] = True
            if where_clause:
                index_kwargs["postgresql_where"] = text(where_clause)

            table_args.append(
                Index(f"ix_{table_name}_{index_name}", *index_columns, **index_kwargs)
            )

        # Process other table-level constraints from schema
        constraints = col_defs.get("_constraints", [])
        for constraint_def in constraints:
            constraint_type = constraint_def["type"]

            if constraint_type == "unique":
                # Unique constraint on multiple columns
                constraint_name = constraint_def.get(
                    "name", f"uq_{table_name}_{constraint_def['columns'][0]}"
                )
                table_args.append(
                    UniqueConstraint(*constraint_def["columns"], name=constraint_name)
                )
            elif constraint_type == "check":
                # Check constraint
                constraint_name = constraint_def.get("name", f"ck_{table_name}")
                table_args.append(
                    CheckConstraint(constraint_def["condition"], name=constraint_name)
                )

        # Add table arguments if any exist
        if table_args:
            attrs["__table_args__"] = tuple(table_args)

        return type(table_name, (self.Base,), attrs)

    def create_gsf_models(self, table_names):
        try:
            for table_name in table_names:
                table_obj = self.create_gsf_model(table_name)
                print(f"adding the new model: {table_name} to {self.db_name}")
                self.models[table_name] = table_obj
                self.valid_table_names.add(table_name)

            self.Base.metadata.create_all(self.engine)
            return {"success": True, "message": "Successfully creating the models"}
        except Exception as e:
            return {"success": False, "message": f"Error creating the models:{str(e)}"}

    @fn_timer
    def load(self, tsv_file, **kwargs):
        LOGGER.info(f"Loading the file: {tsv_file}")
        chunk_size = kwargs.get("chunk_size", 1000)
        start_chunk = kwargs.get("start_chunk")
        chunk_count = kwargs.get("chunk_count")
        skiprows = kwargs.get("skiprows", 0)
        valid_table_name_pat = kwargs.get("valid_table_name_pat")
        skip_table_name_pat = kwargs.get("skip_table_name_pat")
        conflict_action = kwargs.get("on_conflict", "update")
        replacements = kwargs.get("replacements")
        split_columns = kwargs.get("split_columns", ("GENE",))
        exploded_column = kwargs.get("exploded_column", "gene_key")
        hash_columns = kwargs.get("hash_columns")

        if replacements:
            replacements = json.load(open(replacements))

        dtypes = kwargs.get("dtypes", self.default_table_col_python_types)

        chunker = kwargs.get("chunker")

        valid_table_name_regex = (
            re.compile(rf"^{valid_table_name_pat}$", re.I)
            if valid_table_name_pat
            else None
        )
        skip_table_name_regex = (
            re.compile(rf"^{skip_table_name_pat}$", re.I)
            if skip_table_name_pat
            else None
        )

        chunk_idx = 0
        total_inserted = 0
        try:
            file_obj = tsv_file.file if hasattr(tsv_file, "file") else tsv_file
            end_chunk = (
                start_chunk + chunk_count
                if chunk_count is not None and start_chunk is not None
                else None
            )

            gene2chrom_mappings = defaultdict(set)

            if chunker is None:
                chunker = file_chunker_pandas

            for chunk in chunker(
                file_obj,
                dtypes=dtypes,
                chunksize=chunk_size,
                lower_headers=True,
                skiprows=skiprows,
                replacements=replacements,
                numeric_cols=self.default_numeric_col_names,
                split_columns=split_columns,
                exploded_column=exploded_column,
                hash_columns=hash_columns,
            ):
                if start_chunk is not None and chunk_idx < start_chunk:
                    continue
                if end_chunk is not None and chunk_idx >= end_chunk:
                    break

                tab_records = defaultdict(list)

                if not self.partition_col:
                    tab_records[self.default_table_name].extend(chunk)
                else:
                    # group the records by the partition_col (chrom)

                    for rec in chunk:
                        chrom = rec.pop(self.partition_col, None)
                        if not chrom:
                            continue
                        tab_records[chrom].append(rec)
                        gene = rec.get(self.gene_col)
                        if gene:
                            gene2chrom_mappings[gene].add(chrom)

                for chrom, recs in tab_records.items():
                    if (
                        not recs
                        or (
                            valid_table_name_regex
                            and not valid_table_name_regex.match(chrom)
                        )
                        or (
                            skip_table_name_regex and skip_table_name_regex.match(chrom)
                        )
                    ):
                        continue

                    tab_obj = self.models.get(chrom)
                    if not tab_obj:
                        if chrom in self.valid_table_names:
                            tab_obj = Table(
                                chrom, self.metadata, autoload_with=self.engine
                            )
                        else:
                            self.create_gsf_models([chrom])
                            tab_obj = self.models.get(chrom)
                            if not tab_obj:
                                print(
                                    f"Cannot create the new table obj for {chrom}, skip the recs for the table."
                                )
                                continue

                    table = tab_obj if isinstance(tab_obj, Table) else tab_obj.__table__

                    with self.engine.connect() as conn:
                        trans = conn.begin()
                        if self.hash_column_name:
                            uniq_cols = [self.hash_column_name]
                        else:
                            uniq_cols = self.default_unique_index_key_names
                        try:
                            if conflict_action == "ignore":
                                stmt = (
                                    pg_insert(table)
                                    .values(recs)
                                    .on_conflict_do_nothing(index_elements=uniq_cols)
                                )
                                result = conn.execute(stmt)
                            elif conflict_action == "update":
                                update_dict = {
                                    c.name: c
                                    for c in pg_insert(table).excluded
                                    if not c.primary_key
                                }
                                stmt = (
                                    pg_insert(table)
                                    .values(recs)
                                    .on_conflict_do_update(
                                        index_elements=uniq_cols,
                                        set_=update_dict,
                                    )
                                )
                                result = conn.execute(stmt)
                            else:
                                stmt = pg_insert(table).values(recs)
                                result = conn.execute(stmt)

                            total_inserted += result.rowcount

                            trans.commit()
                            chunk_idx += 1
                        except Exception as ce:
                            trans.rollback()
                            LOGGER.error("Error inserting the rows:", str(ce))
                            raise ce

            return {
                "message": "Data loaded successfully. total inserted: {total_inserted}"
            }

        except Exception as e:
            LOGGER.error("Loading error", str(e))
            raise e

    def convert_select_fields(self, selected_fields):
        if not selected_fields:
            selected_fields = "*"
        else:
            # Ensure no empty strings are in the list to prevent trailing commas
            selected_fields = ", ".join(f for f in selected_fields if f)

        return selected_fields

    def search(self, session, **kwargs):
        filters = kwargs.get("filters")
        exclude_cols = kwargs.get("excluded__cols")
        # create the select field string for the select statement
        selected_fields_list = kwargs.get("selected_fields")
        selected_fields = self.convert_select_fields(selected_fields_list)

        size = kwargs.get("size")

        offset = kwargs.get("offset")
        return_lol = kwargs.get("return_lol")

        if not filters:
            print("WARNING: No filter/constraints are provided.")
            if not size:
                raise ValueError("The size has to be specified without a filter")

            filters = [{}]

        if not isinstance(filters, list):
            filters = [filters]

        total_results = []
        warn_msgs = []
        err_msgs = []

        for filter_spec in filters:
            # normalize the gene names to upper case
            genes = filter_spec.get(self.gene_col)
            if genes:
                if isinstance(genes, str):
                    genes = GENE_DELIM_REGX.split(genes)
                gene_list = []
                for gene in genes:
                    gene_upper = gene.upper()
                    gene_list.append(gene_upper)
                filter_spec[self.gene_col] = gene_list

            if self.partition_col is None:
                chroms = [self.default_table_name]
            else:
                chroms = filter_spec.get(self.partition_col)

            if isinstance(chroms, str):
                chroms = [chroms]

            # convert the filter to sql where clause
            where_clause = convert_filter_to_sql_where_clause(
                filter_spec, partition_col=self.partition_col
            )

            for chrom in chroms:
                if exclude_cols:
                    if selected_fields == "*":
                        # remove the cols from the selected fields and add the unique req
                        table = self.models.get(chrom).__table__

                        selected_fields = table.columns

                    # Get all columns except the excluded one
                    selected_fields = [
                        col for col in selected_fields if col.name not in exclude_cols
                    ]

                    distinct_str = " DISTINCT"

                else:
                    distinct_str = ""

                if chrom == self.default_table_name:
                    sql = f'SELECT{distinct_str} {selected_fields} FROM "{chrom}" {where_clause}'
                else:
                    sql = f"SELECT{distinct_str} {selected_fields}, '{chrom}' AS chrom FROM \"{chrom}\" {where_clause}"

                if size:
                    sql += f" LIMIT {size}"
                if offset:
                    sql += f" OFFSET {offset}"

                try:
                    result = session.execute(text(sql))
                    if return_lol:
                        total_results.extend(map(tuple, result))
                    else:
                        total_results.extend([dict(rec) for rec in result])
                except Exception as e:
                    err_msgs.append(
                        f"Error to query {chrom} with {filter_spec} due to: {str(e)}"
                    )

        results = {"results": total_results}
        if warn_msgs:
            results["warnings"] = warn_msgs
        if err_msgs:
            results["errors"] = err_msgs
        return results

    def _drop_table(self, table_name: str):
        """Drop a table if it exists"""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))

        except Exception as e:
            print(f"Error dropping table {table_name}: {e}")
            raise

    def drop_tables(self, table_name_pat, session):
        err_msgs = []
        tn_regex = re.compile(rf"^{table_name_pat}$", re.I)
        for table_name in self.valid_table_names:
            if tn_regex.match(table_name):
                try:
                    session.execute(text(f"DROP table {table_name}"))
                    session.commit()
                    self.valid_table_names.remove(table_name)
                except Exception as e:
                    err_msgs.append(
                        f"Error dropping the table :{table_name} due to:{e}"
                    )
        if err_msgs:
            return {"errors": err_msgs}
        else:
            return {"Status": "Success!"}

    def update_col_values(self, col_name, old2new_mappings):
        try:
            if not self.valid_table_names:
                self.get_curr_table_names()
            if not self.valid_table_names:
                print(f"No table is found in the db {self.db_name}!")
                return

            with self.engine.connect() as conn:
                for table_name in self.valid_table_names:
                    table = Table(table_name, self.metadata, autoload_with=self.engine)
                    for old_val, new_val in old2new_mappings.items():
                        stmt = (
                            update(table)
                            .where(table.c[col_name] == old_val)
                            .values({col_name: new_val})
                        )
                        conn.execute(stmt)
        except Exception as e:
            return {"Updating error": str(e)}


class SV_Database(GSS_Database):
    def __init__(
        self,
        db_name,
        engine=None,
        partition_col=None,
        gene_col="gene",
        table_name="sv_table",
        **kwargs,
    ):
        super().__init__(
            db_name,
            partition_col=partition_col,
            gene_col=gene_col,
            table_name=table_name,
            engine=engine,
            **kwargs,
        )

        # upper acse the col names for loading the csv file
        self.default_table_col_python_types = {
            key.upper(): val for key, val in self.default_table_col_python_types.items()
        }
        self.hash_column_name = "variant_hash"

        if table_name:
            self.default_table_name = table_name

        if partition_col is None:
            # create the table(s)
            self._setup_tables(self.default_table_name)

        self.default_select_text = self.convert_select_fields("*")

    def _setup_tables(self, table_name: str, force_recreate: bool = False):
        """
        Setup tables with various options for handling existing tables.

        :param table_name: Name of the table to create/use
        :param force_recreate: If True, drop and recreate existing tables
        """
        table_exists = self._table_exists(table_name)

        if table_exists and force_recreate:
            # Drop and recreate table
            print(f"Dropping existing table: {table_name}")
            self._drop_table(table_name)
            table_exists = False

        if table_exists:
            # Create model to match existing table
            self.models[table_name] = self.create_gsf_model(
                table_name, **self.default_table_schema
            )

            print(f"Using existing table: {table_name}")
        else:
            # Create new table
            self.models[table_name] = self.create_gsf_model(
                table_name, **self.default_table_schema
            )

            # Create the table
            metadata = self.models[next(iter(self.models))].metadata
            metadata.create_all(self.engine)

            print(f"Created new table: {table_name}")

        # Extract tables reference
        self.tables = {name: model.__table__ for name, model in self.models.items()}

    def load(self, tsv_file, **kwargs):
        # use a customized chunker
        super().load(
            tsv_file,
            chunker=file_chunker_polars,
            hash_columns=self.default_unique_index_key_names,
            **kwargs,
        )

    def convert_select_fields(self, selected_fields, separator=", "):
        """
        Convert the selected field names so that the array type field is converted to a concateneted string

        Args:
            selected_fields: the list of names or just a string
            separator: default separator if array_columns is a list
        """

        # if the selected_fields is "*", use the the table schema to identify the array type
        if not selected_fields or selected_fields == "*":
            # use the pre-created text if available
            if hasattr(self, "default_select_text"):
                return self.default_select_text

            converted_list = []
            for fld, field_def in self.default_table_schema.items():
                if fld == "_indexes" or fld == self.hash_column_name:
                    # skip index defs
                    continue

                if fld == "end":
                    # 'end' is the reserved word of postgres, need to be quoted
                    converted_list.append('"end"')

                elif field_def.get("type") == "array":
                    converted_list.append(
                        f"array_to_string({fld}, '{separator}') AS {fld}"
                    )
                else:
                    converted_list.append(fld)

            return separator.join(converted_list)

        else:
            if isinstance(selected_fields, str):
                selected_fields = selected_fields.lower()

                if selected_fields == "end":
                    return '"end"'

                field_def = self.default_table_schema.get(selected_fields)
                if not field_def:
                    raise ValueError(f"Unknown field name selected: {selected_fields}")

                if field_def.get("type") == "array":
                    return f"array_to_string({selected_fields}, '{separator}') AS {selected_fields}"
                else:
                    return selected_fields

            else:
                converted_list = []
                for fld in selected_fields:
                    fld = fld.lower()
                    if fld == "end":
                        # 'end' is the reserved word of postgres, need to be quoted
                        converted_list.append('"end"')
                        continue

                    field_def = self.default_table_schema.get(fld)
                    if not field_def:
                        print(f"Unknown field name selected: {selected_fields}")
                        continue

                    if field_def.get("type") == "array":
                        converted_list.append(
                            f"array_to_string({fld}, '{separator}') AS {fld}"
                        )
                    else:
                        converted_list.append(fld)

                if not converted_list:
                    raise ValueError(
                        f"Error: None of the selected fields is in the schema: {selected_fields}"
                    )

                return separator.join(converted_list)
