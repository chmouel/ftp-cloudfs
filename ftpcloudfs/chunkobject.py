
import logging
from urllib import quote
from socket import timeout
from ssl import SSLError
from swiftclient.client import ClientException, http_connection

from ftpcloudfs.utils import smart_str

class ChunkObject(object):

    def __init__(self, conn, container, name, content_type=None):
        # FIXME
        # self._name_check()

        parsed, self.chunkable_http = http_connection(conn.url)

        logging.debug("ChunkObject: new connection open (%r, %r)" % (parsed, self.chunkable_http))

        path = '%s/%s/%s' % (parsed.path.rstrip('/'),
                             quote(smart_str(container)),
                             quote(smart_str(name)),
                             )
        headers = { 'X-Auth-Token': conn.token,
                    'Content-Type': content_type or 'application/octet-stream',
                    'Transfer-Encoding': 'chunked',
                    # User-Agent ?
                    }
        if conn.real_ip:
            headers['X-Forwarded-For'] = conn.real_ip
        self.chunkable_http.putrequest('PUT', path)
        for key, value in headers.iteritems():
            self.chunkable_http.putheader(key, value)
        self.chunkable_http.endheaders()
        logging.debug("ChunkedObject: path=%r, headers=%r" % (path, headers))

    def send_chunk(self, chunk):
        logging.debug("ChunkObject: sending %s bytes" % len(chunk))
        try:
            self.chunkable_http.send("%X\r\n" % len(chunk))
            self.chunkable_http.send(chunk)
            self.chunkable_http.send("\r\n")
        except (timeout, SSLError), err:
            raise ClientException(err.message)

    def finish_chunk(self):
        logging.debug("ChunkObject: finish_chunk")
        try:
            self.chunkable_http.send("0\r\n\r\n")
            response = self.chunkable_http.getresponse()
        except (timeout, SSLError), err:
            raise ClientException(err.message)

        try:
            response.read()
        except (timeout, SSLError):
            # this is not relevant, keep going
            pass

        if response.status // 100 != 2:
            raise ClientException(response.reason,
                                  http_status=response.status,
                                  http_reason=response.reason,
                                  )

