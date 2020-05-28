#!/usr/bin/python
import cherrypy
from .common import APPLICATION

cherrypy.quickstart(APPLICATION, '/', {'server.socket_host': '0.0.0.0'})
