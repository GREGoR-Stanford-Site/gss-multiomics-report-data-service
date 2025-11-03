from typing import Dict
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    use_gcp_db: int = Field(
        1,
        alias="USE_GCP_DB",
        description="If true, will use the GCP Cloud SQL database",
    )
    instance_connection_name: str = Field(
        "",
        alias="INSTANCE_CONNECTION_NAME",
        description="The GCP instance connection name",
    )
    postgres_user: str = Field(
        "postgres", alias="POSTGRES_USER", description="The Postgres database user name"
    )
    postgres_password: str = Field(
        "", alias="POSTGRES_PASSWORD", description="The Postgres database user password"
    )
    db_urls: str = Field(alias="DATABASE_URLS")
    db_name: str = Field("db_name", alias="DB_NAME")
    data_dir: str = Field(
        "/app/data/",
        alias="DATA_DIR",
        description="The dir containing supported data files",
    )
    replace_ensembl_ids: int = Field(
        0,
        alias="REPLACE_ENSEMBL_IDS",
        description="If true, will replace the ensembl ids by their gene symbols",
    )
    replace_ensembl_ids_in_db: int = Field(
        0,
        alias="REPLACE_ENSEMBL_IDS_IN_DB",
        description="If true, will replace the ensembl ids by their gene symbols when loading the data to the db",
    )

    @field_validator("db_urls", mode="after")
    @classmethod
    def split_db_urls(cls, val: str) -> Dict[str, str]:
        try:
            if not val:
                print("Empty val, returning empty list")
                return []

            if isinstance(val, str):
                urls = [url.strip() for url in val.split(",") if url.strip()]
                result = {url.split("/")[-1]: url for url in urls}

                return result
            elif isinstance(val, dict):
                return val
            else:
                print(f"Unexpected type: {type(val)}")
                return []
        except Exception as e:
            print(f"Exception in validator: {e}")
            print(f"Exception type: {type(e)}")
            import traceback

            traceback.print_exc()
            raise


try:
    settings = Settings()
    print("Settings created successfully!")
except Exception as e:
    print(f"Error creating settings: {e}")
    raise
