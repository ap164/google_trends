import os
import sys
import pytest
from unittest.mock import patch, mock_open, MagicMock
from pydantic import ValidationError
from unittest.mock import MagicMock, patch
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from utils import (
    load_config, 
    send_email,
    connect_to_postgres,
    PytrendsConfig
)


def test_keywords_max_5_fail():
    with pytest.raises(ValidationError):
        PytrendsConfig(topic="test", keywords=["a", "b", "c", "d", "e", "f"])


@patch("builtins.open", new_callable=mock_open, read_data="""
log_level: INFO
postgres:
  host: localhost
  db: test
  user: user
  password: pass
email:
  sender: test@example.com
  password: secret
pytrends_timeout: [5, 10]
pytrends_configs:
  - topic: "Test"
    keywords: ["a", "b"]
""")
@patch("os.environ.get", return_value="env_value")
@patch("os.path.join", return_value="dummy_path/config.yaml")
@patch("os.path.dirname", return_value="dummy_path")
def test_load_config_success(mock_dirname, mock_join, mock_environ, mock_open_file):
    config = load_config()
    assert config["log_level"] == "INFO"
    assert isinstance(config["pytrends_configs"], list)


@patch("smtplib.SMTP")
def test_send_email_success(mock_smtp):
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    send_email(
        subject="Test",
        message="Hello",
        recipient_email="to@example.com",
        sender_email="from@example.com",
        sender_email_password="password"
    )

    mock_server.login.assert_called_once_with("from@example.com", "password")
    mock_server.sendmail.assert_called()

def test_send_email_missing_credentials(caplog):
    send_email(
        subject="Test",
        message="Hello",
        recipient_email="to@example.com",
        sender_email=None,
        sender_email_password=None
    )
    assert "Missing data to send email" in caplog.text

@patch("psycopg2.connect")
def test_connect_to_postgres_success(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    conn = connect_to_postgres("host", "db", "user", "pass")
    assert conn == mock_conn
    mock_connect.assert_called_once()

@patch("psycopg2.connect", side_effect=Exception("Connection failed"))
def test_connect_to_postgres_failure(mock_connect, caplog):
    conn = connect_to_postgres("host", "db", "user", "pass")
    assert conn is None
    assert "Database connection error" in caplog.text
