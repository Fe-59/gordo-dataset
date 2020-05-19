# -*- coding: utf-8 -*-

import asyncio
import io
import json
import logging
import os
import re
import tempfile
from threading import Lock
from typing import List
from unittest.mock import patch

import docker

import pytest


from gordo_dataset.sensor_tag import SensorTag


from gordo_dataset import sensor_tag
from gordo_dataset.sensor_tag import to_list_of_strings


from tests import utils as tu

logger = logging.getLogger(__name__)

TEST_SERVER_MUTEXT = Lock()

@pytest.fixture(autouse=True)
def check_event_loop():
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        logger.info("Creating new event loop!")
        asyncio.set_event_loop(asyncio.new_event_loop())


@pytest.fixture(scope="session")
def sensors():
    return [SensorTag(f"tag-{i}", None) for i in range(4)]


@pytest.fixture(scope="session")
def sensors_str(sensors):
    return to_list_of_strings(sensors)


@pytest.fixture(scope="session")
def influxdb_name():
    return "testdb"


@pytest.fixture(scope="session")
def influxdb_user():
    return "root"


@pytest.fixture(scope="session")
def influxdb_password():
    return "root"


@pytest.fixture(scope="session")
def influxdb_measurement():
    return "sensors"


@pytest.fixture(scope="session")
def influxdb_fixture_args(sensors_str, influxdb_name, influxdb_user, influxdb_password):
    return (sensors_str, influxdb_name, influxdb_user, influxdb_password, sensors_str)


@pytest.fixture(scope="session")
def influxdb_uri(influxdb_user, influxdb_password, influxdb_name):
    return f"{influxdb_user}:{influxdb_password}@localhost:8086/{influxdb_name}"


@pytest.fixture(scope="session")
def base_influxdb(
    sensors, influxdb_name, influxdb_user, influxdb_password, influxdb_measurement
):
    """
    Fixture to yield a running influx container and pass a tests.utils.InfluxDB
    object which can be used to reset the db to it's original data state.
    """
    client = docker.from_env()

    logger.info("Starting up influx!")
    influx = None
    try:
        influx = client.containers.run(
            image="influxdb:1.7-alpine",
            environment={
                "INFLUXDB_DB": influxdb_name,
                "INFLUXDB_ADMIN_USER": influxdb_user,
                "INFLUXDB_ADMIN_PASSWORD": influxdb_password,
            },
            ports={"8086/tcp": "8086"},
            remove=True,
            detach=True,
        )
        if not tu.wait_for_influx(influx_host="localhost:8086"):
            raise TimeoutError("Influx failed to start")

        logger.info(f"Started influx DB: {influx.name}")

        # Create the interface to the running instance, set default state, and yield it.
        db = tu.InfluxDB(
            sensors,
            influxdb_name,
            influxdb_user,
            influxdb_password,
            influxdb_measurement,
        )
        db.reset()
        logger.info("STARTED INFLUX INSTANCE")
        yield db

    finally:
        logger.info("Killing influx container")
        if influx:
            influx.kill()
        logger.info("Killed influx container")


@pytest.fixture
def influxdb(base_influxdb):
    """
    Fixture to take a running influx and do a reset after each test to ensure
    the data state is the same for each test.
    """
    logger.info("DOING A RESET ON INFLUX DATA")
    base_influxdb.reset()
