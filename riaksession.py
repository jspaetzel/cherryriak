"""
Riak session module for CherryPy by John Spaetzel <http://zzbomb.com/>

Version 0.2. October 18, 2012.

Copyright (c) 2012, John Spaetzel
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
    this list of conditions and the following disclaimer.
    
    * Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
    
    * Neither the name of the Ken Kinder nor the names of its contributors
    may be used to endorse or promote products derived from this software
    without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import riak
import cherrypy
import threading
from datetime import datetime
import json

BUCKET_NAME = 'Session'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class RiakSession(cherrypy.lib.sessions.Session):
    
    def __init__(self, id=None, **kwargs):        
        for k, v in kwargs.items():
            setattr(RiakSession, k, v)

        self.db = self.get_db()
        self.bucket_object = self.db.bucket(BUCKET_NAME)
        
        super(RiakSession, self).__init__(id, **kwargs)
    
    @classmethod
    def get_db(RiakSession):
        from Connect import Connect #import connection object with client object as __self__
        return Connect().client
    
    def _exists(self):
        return self.bucket_object.get(self.id).get_data()
        
    def _load(self):
        data = self.bucket_object.get(self.id).get_data()
        if data:
            time = json.loads(json.dumps(data['datetime']))
            time = datetime.strptime(time, DATETIME_FORMAT)
            return (data['data'], time)
        else:
            return None

    def _save(self, expiration_time):
        meh = self.bucket_object.new(key=self.id, data={ 'data': self._data, 'datetime' : expiration_time.strftime(DATETIME_FORMAT) })
        meh.store()

    def _delete(self):
        self.bucket_object.get(self.id).delete()
        
    def clean_up(self):
        """Clean up expired sessions."""
        now = self.now()
        objs = self.bucket_object.get_keys()
        if objs is not None:
            for x in objs: 
                y=self.bucket_object.get(x)
                if datetime.strptime(y.get_data()['datetime'], DATETIME_FORMAT) < now:
                    y.delete()
                    ## Delete anything with an expiration less then now
    
    def acquire_lock(self):
        self.locked = True

    def release_lock(self):
        self.locked = False
cherrypy.lib.sessions.RiakSession = RiakSession
