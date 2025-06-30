import logging
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, field_validator, Field
from typing import List, Optional, Any, Dict

logger = logging.getLogger(__name__)


class InterestOverTimeConfig(BaseModel):
    """
    Pydantic model for 'interest_over_time' configuration section.
    """
    timeframe: str
    category: Optional[int]
    geo: Optional[str]
    gprop: Optional[str]
    schedule_interval: str

class InterestByRegionConfig(BaseModel):
    """
    Pydantic model for 'interest_by_region' configuration section.
    """
    category: Optional[int]
    geo: Optional[str]
    gprop: Optional[str]
    schedule_interval: str

class PytrendsConfig(BaseModel):
    """
    Pydantic model for a single pytrends configuration.
    """
    topic: str
    keywords: List[str]
    interest_by_region: Optional[InterestByRegionConfig] = None
    interest_over_time: Optional[InterestOverTimeConfig] = None

    @field_validator("keywords")
    def keywords_max_5(cls, v):
        """
        Ensures that the keywords list has at most 5 elements.(api restrictions)
        """
        if len(v) > 5:
            raise ValueError("keywords can have a maximum of 5 elements")
        return v

class Config(BaseModel):
    """
    Pydantic model for the main configuration.
    """
    log_level: str
    postgres: Dict[str, Any]
    email: Dict[str, Any]
    pytrends_timeout: List[int]
    pytrends_configs: List[PytrendsConfig]

# Load environment variables from .env file (if exists)
load_dotenv(dotenv_path="/Users/aniaprus/airflow-docker/.env")

def load_config():
    """
    Loads config.yaml, replaces environment variables and validates types.

    Returns:
        dict: Validated configuration dictionary.
    """
    with open(os.path.join(os.path.dirname(__file__), "config.yaml"), "r") as f:
        config = yaml.safe_load(f)

    def resolve_env_vars(value):
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            return os.environ.get(env_var, value)
        return value

    for section in config:
        if isinstance(config[section], dict):
            for key in config[section]:
                config[section][key] = resolve_env_vars(config[section][key])
        else:
            config[section] = resolve_env_vars(config[section])

    # --- Type validation using Pydantic ---
    try:
        validated = Config(**config)
        return validated.model_dump()
    except ValidationError as e:
        raise RuntimeError(f"config.yaml validation error:\n{e}")
    # --------------------------------------

# Email sending function
def send_email(subject, message, recipient_email, sender_email, sender_email_password):
    """Sends an email with the given subject and content."""
    if not sender_email or not sender_email_password:
        logger.error("Missing data to send email. Check configuration.")
        return

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(message, 'plain'))

    try:
        with smtplib.SMTP('mail.gmx.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_email_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        logger.info(f"Email sent to {recipient_email} with subject: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")

# PostgreSQL connection function
def connect_to_postgres(host, db, user, password):
    """Connects to PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password,
        )
        logger.info("Connected to PostgreSQL database.")
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None