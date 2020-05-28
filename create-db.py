#!/usr/bin/python
import os

import playhouse.db_url
from pacifica.dispatcher.receiver import create_peewee_model

DB_ = playhouse.db_url.connect(os.getenv('DB_URL', 'postgres://podcast:podcast@podcastdb:5432/podcast'))
ReceiveTaskModel = create_peewee_model(DB_)
MODELS_ = (ReceiveTaskModel, )
DB_.create_tables(MODELS_)
