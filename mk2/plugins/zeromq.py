import json

from twisted.internet import protocol
from twisted.internet import reactor

from mk2.plugins import Plugin
from mk2 import events

import zmq
#from txzmq import ZmqEndpoint, ZmqFactory, ZmqPubConnection, ZmqSubConnection ZmqEndpointType
#from txzmq.connection import ZmqEndpointType

from txzmq.connection import ZmqEndpoint, ZmqEndpointType
from txzmq.factory import ZmqFactory
from txzmq.pubsub import ZmqPubConnection


class ZeromqProtocol(protocol.Protocol):
    def __init__(self, parent):
        self.parent = parent

    def request(self, channel, data):
        self.connection.publish(data, channel)

    def encode_request(self, args):
        lines = []
        lines.append('*' + str(len(args)))
        for a in args:
            if isinstance(a, unicode):
                a = a.encode('utf8')
            lines.append('$' + str(len(a)))
            lines.append(a)
        lines.append('')
        return '\r\n'.join(lines)


class ZeromqFactory(protocol.ReconnectingClientFactory):
    def __init__(self, parent, channel):
        self.parent = parent
        self.channel = channel

    def buildProtocol(self, addr):
        self.protocol = ZeromqProtocol(self.parent)
        return self.protocol

    def relay(self, data, channel=None):
        channel = channel or self.channel
        self.protocol.request(channel, json.dumps(data))


class Zeromq(Plugin):
    host = Plugin.Property(default="127.0.0.1")
    port = Plugin.Property(default=5000)
    channel = Plugin.Property(default="mark2-{server}")
    relay_events = Plugin.Property(default="StatPlayers,PlayerJoin,PlayerQuit,PlayerChat,PlayerDeath")
    def setup(self):
        self.endpoint = "tcp://%s:%s" % (self.host, self.port)
        self.factory = ZmqFactory()
        e = ZmqEndpoint(ZmqEndpointType.connect, self.endpoint)
        self.connection = ZmqPubConnection(self.factory, e)

        for ev in self.relay_events.split(','):
            ty = events.get_by_name(ev.strip())
            if ty:
                self.register(self.on_event, ty)
            else:
                self.console("zeromq: couldn't bind to event: {0}".format(ev))

    def on_event(self, event):
        self.factory.relay(event.serialize())

