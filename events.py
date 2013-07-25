###############################################################
## Copyright (c) 2013, Unitclass Inc.
## All rights reserved.
###############################################################

__author__ = 'glo'

try: import simplejson as json
except ImportError: import json

from json import loads

import inspect
import logging
import redis

from socketio.namespace import BaseNamespace
from socketio.mixins import RoomsMixin, BroadcastMixin

REDIS_HOST = 'localhost'
REDIS_CHANNEL_PREFIX = '_channel_'
REDIS_ONLINE_PREFIX = '_online_'

DEBUG = False

class UserIONamespace(BaseNamespace, RoomsMixin):
    def listener(self, chan):
        red = redis.StrictRedis()
        r = red.pubsub()

        chan = str(chan)
        channel = REDIS_CHANNEL_PREFIX + chan
        r.subscribe(channel)

        for m in r.listen():
            try:
                if m['type'] == 'message':
                    data = loads(m['data'])
                    self.socket.send_packet(data)
            except ValueError as err:
                if DEBUG:
                    print err

    def on_subscribe(self, packet, *args, **kwargs):
        # Gets the ARGS of app.socket.of(URI).emit('subscribe', ARGS); from client side
        packet_args = packet['args']
        user_id = packet_args[0]
        user_key = 'user:' + user_id
        self.spawn(self.listener, user_key)

class SocketIONamespace(BaseNamespace, RoomsMixin, BroadcastMixin):

    def listener(self, chan):
        red = redis.StrictRedis()
        r = red.pubsub()

        chan = str(chan)

        channel = REDIS_CHANNEL_PREFIX + chan
        r.subscribe(channel)

        for m in r.listen():
            try:
                if m['type'] == 'message':
                    data = loads(m['data'])
                    self.socket.send_packet(data)
            except ValueError as err:
                if DEBUG:
                    print err

    def is_in_room(self, room):
        """Lets a user join a room on a specific Namespace."""
        _room = self._get_room_name(room)
        if DEBUG:
            print str(_room) + ' ' + str(self.session['rooms'])
        return _room in self.session['rooms']

    def emit_to_channel(self, channel, event, message):
        redis_channel = REDIS_CHANNEL_PREFIX + channel
        red = redis.StrictRedis()
        m = {'type': 'event', 'name': event, 'args':[message], 'endpoint':''}
        red.publish(redis_channel, json.dumps(m))

    def get_users_on_app(self, app_id):
        prefix_app = 'app:'
        app_key = prefix_app + str(app_id)
        redis_key = REDIS_ONLINE_PREFIX + app_key
        red = redis.StrictRedis()
        members = red.smembers(redis_key)
        return members


    def process_event(self, pkt, *args, **kwargs):
        """This function dispatches ``event`` messages to the correct functions.

        Override this function if you want to not dispatch messages
        automatically to "on_event_name" methods.

        If you override this function, none of the on_functions will get called
        by default.
        """
        args = pkt['args']
        name = pkt['name']

        # Process collection IDs:
        id_sep = ':'
        if id_sep in name:
            collection, collection_id = name.split(id_sep, 2)

            kwargs['collection_id'] = collection_id

        method_name = 'on_' + name.replace(' ', '_').replace(':', '_')
        # This means the args, passed as a list, will be expanded to Python args
        # and if you passed a dict, it will be a dict as the first parameter.

        try:
            # Backbone.iosync always sends data in kwargs, either in 'kwargs' or 'args' keys.
            if 'args' in kwargs:
                # Collection read events send data in kwargs['args']
                args = kwargs['args']
            if 'kwargs' in kwargs:
                # Model read events send data in kwargs['kwargs']
                kwargs = kwargs['kwargs']

            #return self.call_method(method_name, pkt, *args)
            method_returns = self.call_method(method_name, pkt, *args)
            # Close one connection here

            return method_returns

        except TypeError:
            logging.error(('Attempted to call event handler %s ' +
                           'with %s args and %s kwargs.') % (method_name, repr(args), repr(kwargs)))
            raise

    def call_method(self, method_name, packet, *args):
        """This function is used to implement the two behaviors on dispatched
        ``on_*()`` and ``recv_*()`` method calls.

        Those are the two behaviors:

        * If there is only one parameter on the dispatched method and
          it is equal to ``packet``, then pass in the packet as the
          sole parameter.

        * Otherwise, pass in the arguments as specified by the
          different ``recv_*()`` methods args specs, or the
          :meth:`process_event` documentation.
        """
        method = getattr(self, method_name, None)
        if method is None:
            self.error('no_such_method',
                'The method "%s" was not found' % method_name)
            return

        specs = inspect.getargspec(method)
        func_args = specs.args
        if not len(func_args) or func_args[0] != 'self':
            self.error("invalid_method_args",
                "The server-side method is invalid, as it doesn't "
                "have 'self' as its first argument")
            return
        if len(func_args) == 2 and func_args[1] == 'packet':
            return method(packet)
        else:
            return method(*args)