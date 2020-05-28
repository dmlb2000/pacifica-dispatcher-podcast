#!/usr/bin/python
import os
import sys
import json
import contextlib
import hashlib
import tempfile
import threading
from io import StringIO

import requests
import cherrypy
import playhouse.db_url
from cloudevents.model import Event
from jsonpath2.path import Path
from celery.utils.log import mlevel

from pacifica.downloader import Downloader
from pacifica.uploader import Uploader

from pacifica.dispatcher.models import File, Transaction, TransactionKeyValue
from pacifica.dispatcher.event_handlers import EventHandler
from pacifica.dispatcher.receiver import create_peewee_model
from pacifica.dispatcher.router import Router
from pacifica.dispatcher.downloader_runners import DownloaderRunner, RemoteDownloaderRunner
from pacifica.dispatcher.uploader_runners import UploaderRunner, RemoteUploaderRunner

DB_ = playhouse.db_url.connect(os.getenv('DB_URL', 'postgres://podcast:podcast@podcastdb:5432/podcast'))
ReceiveTaskModel = create_peewee_model(DB_)
MODELS_ = (ReceiveTaskModel, )
ROUTER = Router()


class SimpleEventHandler(EventHandler):
    def __init__(self, downloader_runner: DownloaderRunner, uploader_runner: UploaderRunner) -> None:
        """Save the download and upload runner classes for later use."""
        super(SimpleEventHandler, self).__init__()
        self.downloader_runner = downloader_runner
        self.uploader_runner = uploader_runner

    def handle(self, event: Event) -> None:
        """
        Example handle event.

        This handler downloads all files in the event.
        Converts the files to uppercase and uploads them back to Pacifica.
        """
        transaction_inst = Transaction.from_cloudevents_model(event)
        transaction_key_value_insts = TransactionKeyValue.from_cloudevents_model(event)
        file_insts = File.from_cloudevents_model(event)
        with tempfile.TemporaryDirectory() as downloader_tempdir_name:
            with tempfile.TemporaryDirectory() as uploader_tempdir_name:
                for file_opener in self.downloader_runner.download(downloader_tempdir_name, file_insts):
                    with file_opener() as file_fd:
                        with open(os.path.join(uploader_tempdir_name, file_fd.name), 'w') as wfile_fd:
                            wfile_fd.write(file_fd.read().upper())
                (_bundle, _job_id, _state) = self.uploader_runner.upload(
                    uploader_tempdir_name, transaction=Transaction(
                        submitter=transaction_inst.submitter,
                        instrument=transaction_inst.instrument,
                        project=transaction_inst.project
                    ), transaction_key_values=[
                        TransactionKeyValue(key='uppercase_text', value='True'),
                        TransactionKeyValue(key='Transactions._id', value=transaction_inst._id)
                    ]
                )

ROUTER.add_route(
    Path.parse_str("""
        $["data"][*][?(
            @["destinationTable"] = "TransactionKeyValue" and
            @[\"key\"] = \"uppercase_text\" and
            @[\"value\"] = \"False\"
          )]
    """),
    SimpleEventHandler(
        RemoteDownloaderRunner(Downloader()), RemoteUploaderRunner(Uploader())
    )
)

CELERY_APP = ReceiveTaskModel.create_celery_app(
    ROUTER,
    'pacifica.dispatcher.app',
    'pacifica.dispatcher.tasks.receive',
    backend='rpc://',
    broker=os.getenv('BROKER_URL', 'redis://redis:6379/0')
)
APPLICATION = ReceiveTaskModel.create_cherrypy_app(CELERY_APP.tasks['pacifica.dispatcher.tasks.receive'])
