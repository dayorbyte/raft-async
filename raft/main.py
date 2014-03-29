import sys
from raft.server import RaftServer, RaftHandler
from raft.peer import Peer

def main():
    # Create the event loop
    loop = asyncio.get_event_loop()

    # Create the raft log
    log = RaftLog()

    ports = range(8080, 8080+int(sys.argv[1]))
    for port in ports:
        try:
            peers = [Peer('localhost', port)
                      for p in ports if p != port]
            raft_server = RaftServer('localhost', port, log, peers)
            def RaftProtocol():
                return RaftHandler(server)
            coro = loop.create_server(RaftProtocol, 'localhost', port)
            server = loop.run_until_complete(coro)
            # server.listen()
            print('listening on: ' + str(8000 + port))
            server.hosts = [('localhost', (8000 + offset))
                             for o in port_offsets if offset != port]
            break
        except OSError:
            pass
        except Exception as e:
            traceback.print_exc()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("exit")
    finally:
        server.close()
        loop.close()

if __name__ == '__main__':
    main(sys.argv[1:])