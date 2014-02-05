import json
from twisted.internet import protocol
from twisted.internet import reactor
from mk2.plugins import Plugin
from mk2 import events
import zmq
from txzmq.connection import ZmqEndpoint, ZmqEndpointType
from txzmq.factory import ZmqFactory
from txzmq.pubsub import ZmqPubConnection


class Zeromq(Plugin):
    host = Plugin.Property(default="localhost")
    port = Plugin.Property(default=5000)
    channel = Plugin.Property(default="mark2-{server}")
    relay_events = Plugin.Property(default="StatPlayers,PlayerJoin,PlayerQuit,PlayerChat,PlayerDeath")
    def setup(self):
        endpoint = "tcp://%s:%s" % (self.host, self.port)
        zf = ZmqFactory()
        ze = ZmqEndpoint(ZmqEndpointType.bind, endpoint)
        self.zcon = ZmqPubConnection(zf, ze)

        for ev in self.relay_events.split(','):
            ty = events.get_by_name(ev.strip())
            if ty:
                self.register(self.on_event, ty)
            else:
                self.console("zeromq: couldn't bind to event: {0}".format(ev))

    def on_event(self, event):
        self.console("zeromq: " + self.channel)
        self.zcon.publish(json.dumps(event.serialize()), self.channel)

